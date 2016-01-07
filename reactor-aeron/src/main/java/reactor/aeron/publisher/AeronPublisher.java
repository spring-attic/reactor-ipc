/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.aeron.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.support.Logger;
import reactor.Timers;
import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.aeron.support.ServiceMessagePublicationFailedException;
import reactor.aeron.support.ServiceMessageType;
import reactor.core.support.ReactiveState;
import reactor.core.support.SingleUseExecutor;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.core.timer.Timer;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The publisher part of Reactive Streams over Aeron transport implementation
 * used for receiving signals sent over Aeron from {@link reactor.aeron.subscriber.AeronSubscriber}
 * and configured via fields of {@link Context}.
 * <br/>The publisher supports both unicast and multicast modes of {@link reactor.aeron.subscriber.AeronSubscriber}
 * and uses the same streamIds.
 *
 * <br/>To configure the publisher in uncast mode of operation set {@link Context#senderChannel} to the value used for
 * the subscriber configuration and {@link Context#receiverChannel} should be set to
 * "udp://" + &lt;Network Interface IP Address&gt; + ":&lt;Port&gt;", where
 * Network Interface IP Address is the IP address of a network interface used for communication over the network
 * with the signals sender, Port - is a UDP port on which to listen.
 * <br/>For example, senderChannel - "udp://serverbox:12000", receiverChannel - "udp://clientbox:12001".
 *
 * <p/>
 * Only a single subscriber to the publisher is supported.
 *
 * <p/>Quck start example:
 * <pre>
 *     AeronPublisher publisher = AeronPublisher.create(new Context()
 *         .name("publisher").senderChannel("udp://serverbox:12000).receiverChannel("udp://clientbox:12001"));
 * </pre>
 *
 * @author Anatoly Kadyshev
 * @since 2.5
 */
public class AeronPublisher implements Publisher<Buffer>, ReactiveState.Downstream {

	private static final Logger logger = Logger.getLogger(AeronPublisher.class);

	final AeronInfra aeronInfra;

	private final Context context;

	private final ExecutorService executor;

	private final Runnable shutdownTask;

	private final Runnable onTerminateTask;

	private final Publication serviceRequestPub;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private volatile boolean terminated = false;

	private final HeartbeatSender heartbeatSender;

	private final ServiceMessageSender serviceMessageSender;

	private volatile AtomicBoolean subscribed = new AtomicBoolean(false);

	private volatile SignalPoller signalPoller;

	private final String sessionId;

	public static AeronPublisher create(Context context) {
		context.validate();
		return new AeronPublisher(context);
	}

	public AeronPublisher(Context context,
						  Runnable shutdownTask,
						  Runnable onTerminateTask) {

		if (shutdownTask == null) {
			shutdownTask = new Runnable() {
				@Override
				public void run() {
					shutdown();
				}
			};
		}

		this.context = context;
		this.onTerminateTask = onTerminateTask;
		this.aeronInfra = context.createAeronInfra();
		this.executor = SingleUseExecutor.create(AeronUtils.makeThreadName(context, "signal-poller"));
		this.serviceRequestPub = createServiceRequestPub(context, this.aeronInfra);
		this.sessionId = getSessionId(context);
		this.serviceMessageSender = new ServiceMessageSender(this, serviceRequestPub, sessionId);
		this.heartbeatSender = new HeartbeatSender(context,
				new ServiceMessageSender(this, serviceRequestPub, sessionId), new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				shutdown();
			}
		});
		this.shutdownTask = shutdownTask;

		logger.info("publisher initialized, sessionId: {}", sessionId);
	}

	protected String getSessionId(Context context) {
		return AeronUtils.isMulticastCommunication(context) ?
				UUIDUtils.create().toString():
				context.receiverChannel() + "/" + context.streamId() + "/" + context.errorStreamId();
	}

	public AeronPublisher(Context context) {
		this(context,
				null,
				new Runnable() {
					@Override
					public void run() {
					}
				});

	}

	private Publication createServiceRequestPub(Context context, AeronInfra aeronInfra) {
		return aeronInfra.addPublication(context.senderChannel(), context.serviceRequestStreamId());
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		if (subscriber == null) {
			throw Exceptions.spec_2_13_exception();
		}

		if (!subscribed.compareAndSet(false, true)) {
			throw new IllegalStateException("Only single subscriber is supported");
		}

		signalPoller = createSignalsPoller(subscriber);
		try {
			executor.execute(signalPoller);
			heartbeatSender.start();
		} catch (Throwable t) {
			signalPoller = null;
			subscribed.set(false);
			subscriber.onError(new RuntimeException("Failed to schedule poller for signals", t));
		}
	}

	private SignalPoller createSignalsPoller(final Subscriber<? super Buffer> subscriber) {
		return new SignalPoller(context, serviceMessageSender, subscriber, aeronInfra, new Consumer<Boolean>() {

			@Override
			public void accept(Boolean isTerminalSignalReceived) {
				heartbeatSender.shutdown();

				terminateSession();

				signalPoller = null;
				subscribed.set(false);

				if (context.autoCancel() || isTerminalSignalReceived) {
					shutdownTask.run();
				}
			}
		});
	}

	private void terminateSession() {
		try {
			serviceMessageSender.sendCancel();
		} catch (Exception e) {
			context.errorConsumer().accept(
					new ServiceMessagePublicationFailedException(ServiceMessageType.Cancel, e));
		}
	}

	public void shutdown() {
		if (alive.compareAndSet(true, false)) {
			// Doing a shutdown via timer to avoid shutting down Aeron in its thread
			final Timer globalTimer = Timers.global();
			globalTimer.submit(new Consumer<Long>() {
				@Override
				public void accept(Long value) {
					if (signalPoller != null) {
						signalPoller.shutdown();
					}
					executor.shutdown();

					globalTimer.submit(new Consumer<Long>() {
						@Override
						public void accept(Long aLong) {
							if (!executor.isTerminated()) {
								globalTimer.submit(this);
								return;
							}

							aeronInfra.close(serviceRequestPub);
							aeronInfra.shutdown();

							logger.info("publisher shutdown, sessionId: {}", sessionId);
							terminated = true;

							onTerminateTask.run();
						}
					});
				}
			});
		}
	}

	@Override
	public Object downstream() {
		return signalPoller;
	}

	public boolean alive() {
		return alive.get();
	}

	public boolean isTerminated() {
		return terminated;
	}

}
