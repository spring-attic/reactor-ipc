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
package reactor.aeron.subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.core.publisher.ProcessorTopic;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.Logger;
import reactor.core.support.ReactiveState;
import reactor.core.timer.Timer;
import reactor.core.timer.Timers;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;

/**
 * The subscriber part of Reactive Stream over Aeron transport implementation
 * used to pass signals to publishers {@link reactor.aeron.publisher.AeronPublisher} over Aeron
 * and configured via fields of {@link Context}.
 *
 * <p/>Can operate in both unicast and multicast sending modes.
 *
 * <br/>First of all, to start using the functionality you need to set
 * {@link Context#senderChannel(String)} to "udp://&lt;Network Interface IP Address&gt;:&lt;Port&gt;",
 * where Network Interface IP Address is the IP address of a network interface used to communicate with signal
 * receivers over the network. It can also be a machine name on the network as it's resolved to the corresponding
 * network interface IP.
 * <br/>For example, "udp://serverbox:12000"<br/>
 * where serverbox is a machine name in the network corresponding to a network interface IP Address
 * and 12000 is a UDP port on which to listen for subscription requests.
 * <br/>To configure the subscriber in unicast mode leave field {@link Context#senderChannel(String)} blank.
 * <br/>To configure the subscriber in multicast mode set field {@link Context#senderChannel(String)}
 * to a multicast channel, e.g. "udp://239.1.1.1:12001" where 239.1.1.1 is a multicast IP address.
 *
 * <p/>
 * The subscriber uses 3 <b>different</b> pre-configured Aeron streamIds to function:<br>
 * <ul>
 *     <li>{@link Context#streamId} - used for passing of Next and Complete signals from
 *     the signals sender to signals receivers</li>
 *     <li>{@link Context#errorStreamId} - for passing of Error signals</li>
 *     <li>{@link Context#serviceRequestStreamId} - for service requests of
 *     {@link reactor.aeron.support.ServiceMessageType}
 *     from signal receivers to the signal sender
 * </ul>
 *
 * The streamIds are set by default and normally there is no need to set them manually
 * unless you need to launch a new instance of the subscriber on the same channel.
 *
 * <p>The subscriber could use an external Aeron instance provided via {@link Context#aeron} field.
 * If no exteranl Aeron instance is provided the subscriber will use an embedded Media driver.
 *
 * <p>When the Aeron buffer for published messages becomes completely full
 * the subscriber starts to throttle and as a result method
 * {@link #onNext(Buffer)} blocks until messages are consumed or
 * {@link Context#publicationRetryMillis} timeout elapses.
 * If a message cannot be published into Aeron within
 * {@link Context#publicationRetryMillis} the corresponding exception is provided into {@link Context#errorConsumer}.
 *
 * <p>When auto-cancel is enabled via {@link Context#autoCancel} and the last signal receiver disconnects
 * an upstream subscription to the upstream publisher is cancelled.
 *
 * <p>When a signal receiver requests {@link Long#MAX_VALUE} there
 * won't be any backpressure applied and thread publishing into the subscriber
 * will run at risk of being throttled if signal receivers don't catch up.<br>
 *
 * <p>The subscriber created via {@link #create(Context)} methods respects the Reactive Stream contract
 * and must not be signalled concurrently on any onXXXX methods.
 * <br/>Nonetheless Reactor allows creating of a subscriber which can be used by
 * publishers from different threads. In this case the subscriber should be
 * created via {@link #share(Context)} methods.
 *
 * <p/>Quck start example:
 * <pre>
 *     AeronSubscriber subscriber = AeronSubscriber.create(new Context()
 *         .name("subscriber").senderChannel("udp://serverbox:12000));
 * </pre>
 *
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 * @since 2.5
 */
public class AeronSubscriber extends BaseSubscriber<Buffer>
		implements ReactiveState.ActiveUpstream, ReactiveState.Upstream, ReactiveState.FeedbackLoop {

	private static final Logger logger = Logger.getLogger(AeronSubscriber.class);

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private volatile boolean terminated = false;

	private final Runnable onTerminateTask;

	private final AeronInfra aeronInfra;

	private final ServiceMessageHandler serviceMessageHandler;

	private final ServiceMessagePoller serviceMessagePoller;

	private final ProcessorTopic<Buffer> processor;

	public static AeronSubscriber create(Context context) {
		context.validate();
		return new AeronSubscriber(context, false);
	}

	public static AeronSubscriber share(Context context) {
		context.validate();
		return new AeronSubscriber(context, true);
	}

	public AeronSubscriber(Context context,
						   boolean multiPublishers,
			Runnable shutdownTask,
			Runnable onTerminateTask) {
		this.onTerminateTask = onTerminateTask;

		if (shutdownTask == null) {
			shutdownTask = new Runnable() {
				@Override
				public void run() {
					shutdown();
				}
			};
		}

		this.aeronInfra = context.createAeronInfra();
		this.processor = createProcessorTopic(context, multiPublishers);
		boolean isMulticast = AeronUtils.isMulticastCommunication(context);
		if (isMulticast) {
			this.serviceMessageHandler = new MulticastServiceMessageHandler(processor, aeronInfra, context,
					shutdownTask);
		} else {
			this.serviceMessageHandler = new UnicastServiceMessageHandler(processor, aeronInfra, context,
					shutdownTask);
		}
		this.serviceMessagePoller = createServiceMessagePoller(context, aeronInfra, serviceMessageHandler);

		//TODO: Do not start from constructor
		serviceMessageHandler.start();
		serviceMessagePoller.start();

		logger.info("subscriber initialized in {} mode, service request channel/streamId: {}",
				isMulticast ? "multicast" : "unicast",
				context.senderChannel() + "/" + context.serviceRequestStreamId());
	}

	public AeronSubscriber(Context context, boolean multiPublishers) {
		this(context,
				multiPublishers,
				null,
				new Runnable() {
					@Override
					public void run() {
					}
				});
	}

	@Override
	public void onSubscribe(Subscription s) {
		super.onSubscribe(s);

		processor.onSubscribe(s);
	}

	@Override
	public void onNext(Buffer buffer) {
		super.onNext(buffer);

		processor.onNext(buffer);
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);

		processor.onError(t);
	}

	@Override
	public void onComplete() {
		super.onComplete();

		processor.onComplete();
	}

	private ServiceMessagePoller createServiceMessagePoller(Context context, AeronInfra aeronInfra,
															ServiceMessageHandler serviceMessageHandler) {
		return new ServiceMessagePoller(context, aeronInfra, serviceMessageHandler);
	}

	private ProcessorTopic<Buffer> createProcessorTopic(Context context, boolean multiPublishers) {
		String name = AeronUtils.makeThreadName(context, "signal-sender");
		return multiPublishers ?
				ProcessorTopic.<Buffer>share(name, context.ringBufferSize(), context.autoCancel()) :
				ProcessorTopic.<Buffer>create(name, context.ringBufferSize(), context.autoCancel());
	}

	public void shutdown() {
		if (alive.compareAndSet(true, false)) {
			// Doing a shutdown via globalTimer to avoid shutting down Aeron in its thread
			final Timer globalTimer = Timers.global();
			globalTimer.submit(new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					processor.shutdown();

					serviceMessagePoller.shutdown();
					serviceMessageHandler.shutdown();

					// Waiting till service message poller is terminated to safe shutdown Aeron
					globalTimer.submit(new Consumer<Long>() {
						@Override
						public void accept(Long aLong) {
							if (!serviceMessagePoller.isTerminated()) {
								globalTimer.submit(this);
								return;
							}

							aeronInfra.shutdown();

							logger.info("subscriber shutdown");
							terminated = true;

							onTerminateTask.run();
						}
					});
				}
			});
		}
	}

	@Override
	public boolean isTerminated() {
		return terminated;
	}

	@Override
	public boolean isStarted() {
		return alive.get();
	}
	@Override
	public Object delegateInput() {
		return serviceMessageHandler;
	}

	@Override
	public Object upstream() {
		return processor;
	}

	@Override
	public Object delegateOutput() {
		return serviceMessagePoller;
	}
}
