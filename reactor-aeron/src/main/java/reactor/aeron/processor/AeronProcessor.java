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
package reactor.aeron.processor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.aeron.Context;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.support.ServiceMessageType;
import reactor.core.processor.BaseProcessor;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A processor which publishes into and subscribes to data from Aeron.<br>
 * For more information about Aeron go to
 * <a href="https://github.com/real-logic/Aeron">Aeron Project Home</a>
 *
 * <p>
 * The processor plays roles of both {@link Publisher} and
 * {@link Subscriber}<br>
 * <ul>
 * <li>{@link Subscriber} part of the processor called as
 * <b>'signals sender'</b> below publishes messages into Aeron.</li>
 * <li>{@link Publisher} part of the processor called as
 * <b>'signals receiver'</b> below subscribers for messages published
 * by the sender part.</li>
 * </ul>
 *
 * <p>
 * An instance of the processor is created upon a single Aeron channel of
 * {@link Context#channel} and requires 4 <b>different</b> streamIds to function:<br>
 * <ul>
 *     <li>{@link Context#streamId} - used for sending Next and Complete signals from
 *     the signals sender to the signals receiver</li>
 *     <li>{@link Context#errorStreamId} - for Error signals</li>
 *     <li>{@link Context#serviceRequestStreamId} - for service requests of {@link ServiceMessageType}
 *     from the signals receiver to the signals sender
 * </ul>
 *
 * <p>
 * The processor could launch an embedded Media Driver for the application
 * if requested via <code>launchEmbeddedMediaDriver</code> parameter during
 * the processor creation via static methods or via
 * {@link Context#launchEmbeddedMediaDriver(boolean)} when created using the
 * {@link Context}.<br>
 * Only a single instance of the embedded media driver is launched for the
 * application.<br>
 * The launched Media Driver instance is shut down once the last
 * instance of {@link AeronProcessor} is shut down.
 *
 * <p>
 * The processor created via {@link #create(String, boolean, String)}
 * or {@link #create(Context)} methods respects the Reactive Streams contract
 * and must not be signalled concurrently on any onXXXX methods.<br>
 * Nonetheless Reactor allows creating of a processor which can be used by
 * publishers from different threads. In this case the processor should be
 * created via either {@link #share(String, boolean, String)}
 * or {@link #share(Context)} methods.
 *
 * <p>
 * Each subscriber is assigned a unique thread that stops either on
 * the processor subscription cancellation or upon a terminal event of Complete or
 * Error.
 *
 * <p>
 * When auto-cancel is enabled and the last subscriber is unregistered
 * an upstream subscription to the upstream publisher is cancelled.
 *
 * <p>
 * The processor could be assigned a custom executor service when is
 * constructed via {@link Context}. The executor service decides upon threads
 * allocation for the processor subscribers.
 *
 * <p>
 * When a Subscriber to the processor requests {@link Long#MAX_VALUE} there
 * won't be any backpressure applied and thread publishing into the processor
 * will run at risk of being throttled if subscribers don't catch up.<br>
 * With any other strictly positive demand a subscriber will stop reading new
 * Next signals (Complete and Error will still be read) as soon as the demand
 * has been fully consumed.
 *
 * <p>
 * When more than 1 subscriber listens to the processor they all receive
 * the exact same events if their respective demand is still strictly positive,
 * very much like a Fan-Out scenario.
 *
 * <p>
 * When the Aeron buffer for published messages becomes completely full
 * the processor starts to throttle and as a result method
 * {@link #onNext(Buffer)} blocks until messages are consumed or
 * {@link Context#publicationRetryMillis} timeout elapses.<br>
 *
 * If a message cannot be published into Aeron within
 * {@link Context#publicationRetryMillis} then it is discarded.
 * In the next version of the processor this behaviour is likely to change.<br>
 *
 * For configuration of Aeron buffers refer to
 * <a href="https://github.com/real-logic/Aeron/wiki/Configuration-Options">Aeron Configuration Options</a>
 *
 * <p>
 * Instances of {@link AeronProcessor} can communicate with each other over a
 * network.<br>
 * In this case {@link Context#channel} should be a multicast channel. For more information
 * regarding Aeron channels configuration please refer to
 * <a href="https://github.com/real-logic/Aeron/wiki/Channel-Configuration">Channel Coniguration</a>.
 * <br>
 * For example, for a typical client-server application an instance of
 * the processor called the server and an instance of the processor called the
 * client are located on different machines. Both the server and the client
 * are configured with the same multicast channel and streamIds.
 * In this case a client subscriber requests of {@link Subscription#request(long)}
 * and {@link Subscription#cancel()} and sent to the server.
 * The server in its turn invokes them on its upstream subscription and sends
 * data to the client.<br>
 * A sample server instance:
 * <pre>
 * AeronProcessor server = AeronProcessor.create("server", true, true, "udp://239.1.1.1:12001", 1, 2, 3, 4);
 * serverSidePublisher.subscribe(server);
 * </pre>
 *
 * And a client instance:
 * <pre>
 * AeronProcessor client = AeronProcessor.create("client", true, true, "udp://239.1.1.1:12001", 1, 2, 3, 4);
 * client.subscribe(clientSideSubscriber);
 * </pre>
 *
 * In the example above when <tt>clientSideSubscriber</tt> calls
 * {@link Subscription#request(long)} or {@link Subscription#cancel()}
 * methods on its subscription provided by the processor the calls are sent to
 * the server instance which invokes them on the upstream subscription provided
 * by <tt>clientSidePublisher</tt>. As a result data pushed by
 * <code>clientSidePublisher</code> is sent to the client.
 *
 * @author Anatoly Kadyshev
 */
public class AeronProcessor extends BaseProcessor<Buffer, Buffer> {

	private static final Logger logger = LoggerFactory.getLogger(AeronProcessor.class);

	/**
	 * Reactive Publisher part of the processor - signals sender
	 */
	private final AeronPublisher publisher;

	/**
	 * Reactive Subscriber part of the processor - signals receiver
	 */
	private final AeronSubscriber subscriber;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	/**
	 * Creates a new processor using the context
	 *
	 * @param context configuration of the processor
	 */
	AeronProcessor(Context context, boolean multiPublishers) {
		super(context.autoCancel());
		this.subscriber = createAeronSubscriber(context, multiPublishers);
		this.publisher = createAeronPublisher(context);
	}

	private AeronPublisher createAeronPublisher(Context context) {
		return new AeronPublisher(context, logger, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				shutdown();
			}
		});
	}

	private AeronSubscriber createAeronSubscriber(Context context, boolean multiPublishers) {
		return new AeronSubscriber(
				context,
				logger,
				multiPublishers,
				new Consumer<Void>() {
					@Override
					public void accept(Void value) {
						shutdown();
					}
				});
	}

	public static AeronProcessor create(Context context) {
		context.validate();
		return new AeronProcessor(context, false);
	}


	public static AeronProcessor share(Context context) {
		context.validate();
		return new AeronProcessor(context, true);
	}

	/**
	 * Returns available capacity for the number of messages which can be sent via the processor.
	 * Not implemented yet and always returns 0.
	 *
	 * @return 0
	 */
	@Override
	public long getAvailableCapacity() {
		return 0;
	}

	@Override
	public void onSubscribe(Subscription s) {
		super.onSubscribe(s);

		subscriber.onSubscribe(upstreamSubscription);
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		publisher.subscribe(subscriber);
	}

	/**
	 * Publishes Next signal containing <code>buffer</code> into Aeron.
	 *
	 * @param buffer buffer to be published
	 */
	@Override
	public void onNext(Buffer buffer) {
		subscriber.onNext(buffer);
	}

	@Override
	public void onError(Throwable t) {
		subscriber.onError(t);
	}

	@Override
	public void onComplete() {
		subscriber.onComplete();
	}

	public void shutdown() {
		if (alive.compareAndSet(true, false)) {
			subscriber.shutdown();
			publisher.shutdown();
			logger.info("processor shutdown");
		}
	}

	public boolean alive() {
		return alive.get();
	}

	public boolean isTerminated() {
		return subscriber.isTerminated() && publisher.isTerminated();
	}

}