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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.aeron.Context;
import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.ServiceMessagePublicationFailedException;
import reactor.aeron.utils.ServiceMessageType;
import reactor.core.Producer;
import reactor.util.Loggers;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.ipc.util.UUIDUtils;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;

/**
 * The publisher part of Reactive Streams over Aeron transport implementation
 * used for receiving signals sent over Aeron from {@link reactor.aeron.subscriber.AeronSubscriber}
 * and configured via fields of {@link Context}.
 * <br/>The publisher supports both unicast and multicast modes of {@link reactor.aeron.subscriber.AeronSubscriber}
 * and uses the same streamIds.
 *
 * <br/>To configure the publisher in uncast mode of operation set {@link Context#senderChannel} to the value used for
 * the subscriber configuration and {@link Context#receiverChannel} should be set to
 * "udp://&lt;Network Interface IP Address&gt;:&lt;Port&gt;", where
 * Network Interface IP Address is the IP address of a network interface used for communication over the network
 * with the signals sender, Port - is a UDP port on which to listen.
 * <br/>For example, senderChannel - "udp://serverbox:12000", receiverChannel - "udp://clientbox:12001".
 *
 * <p/>
 * Only a single subscriber to the publisher is supported.
 *
 * <p/>Quck start example:
 * <pre>
 *     AeronFlux publisher = AeronFlux.create(new Context()
 *         .name("publisher").senderChannel("udp://serverbox:12000).receiverChannel("udp://clientbox:12001"));
 * </pre>
 *
 * Communication with the subscriber is performed within a session identified by SessionId.
 * A subscriber instance supports several independent publishers connecting to it and
 * tracks demand of each publisher separately per session.
 *
 * <p/>
 * Technical implementation details.
 *
 * <p/>For unicast communication SessionId has the following format:
 * <tt>receiverChannel/streamId/errorStreamId</tt>
 * <br/>e.g.: <tt>udp://clientbox:12001/1/2</tt>
 *
 * <p/>For multicast communication SessionId has the the following format:
 * <tt>uuid</tt>
 * <br/>where <tt>uuid</tt> is a unique id of a publisher on the network.
 * <br/>e.g.: <tt>50f50900-c1b7-11e5-730c-8c6851c29e9d</tt>
 *
 * @author Anatoly Kadyshev
 * @since 2.5
 */
public final class AeronFlux extends Flux<Buffer> implements Producer {

	private static final Logger logger = Loggers.getLogger(AeronFlux.class);

	final AeronInfra aeronInfra;

	private final Context context;

	private final ExecutorService executor;

	private final Publication serviceRequestPub;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private volatile boolean terminated = false;

	private final HeartbeatSender heartbeatSender;

	private final ServiceMessageSender serviceMessageSender;

	private final AtomicBoolean subscribed = new AtomicBoolean(false);

	private volatile SignalPoller signalPoller;

	private final String sessionId;

	/**
	 * Creates a {@link Flux} for receiving signals from Aeron
	 *
	 * @param context settings for connecting to an instance of signals sender
	 *
	 * @return a new Flux receiving signals from Aeron
	 */
	public static Flux<Buffer> listenOn(Context context) {
		return new AeronFlux(context);
	}

	protected AeronFlux(Context context) {
		Objects.requireNonNull(context.receiverChannel(), "'receiverChannel' should be provided");
		context.validate();

		this.context = context;
		this.aeronInfra = context.aeronInfra();
		this.executor =
				Executors.newCachedThreadPool(r -> new Thread(r, AeronUtils.makeThreadName(
						context.name(),
						"publisher",
						"signal-poller")));
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

		logger.info("publisher initialized, sessionId: {}", sessionId);
	}

	protected String getSessionId(Context context) {
		return AeronUtils.isMulticastCommunication(context) ?
				UUIDUtils.create().toString():
				context.receiverChannel() + "/" + context.streamId();
	}

	private Publication createServiceRequestPub(Context context, AeronInfra aeronInfra) {
		return aeronInfra.addPublication(context.senderChannel(), context.serviceRequestStreamId());
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		if (subscriber == null) {
			throw Exceptions.argumentIsNullException();
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
		return new SignalPoller(context, serviceMessageSender, subscriber, aeronInfra, () -> {
            heartbeatSender.shutdown();

            terminateSession();

            signalPoller = null;
            subscribed.set(false);

            shutdown();
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
			final TimedScheduler globalTimer = Schedulers.timer();
			globalTimer.schedule(() -> {
					if (signalPoller != null) {
						signalPoller.shutdown();
					}
					executor.shutdown();

					globalTimer.schedule(new Runnable() {
						@Override
						public void run() {
							if (!executor.isTerminated()) {
								globalTimer.schedule(this);
								return;
							}

							aeronInfra.close(serviceRequestPub);
							aeronInfra.shutdown();

							logger.info("publisher shutdown, sessionId: {}", sessionId);
							terminated = true;
						}
					});
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
