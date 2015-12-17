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

import org.reactivestreams.Subscription;
import reactor.core.support.Logger;
import reactor.core.support.Logger;
import reactor.Timers;
import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.core.timer.Timer;
import reactor.io.buffer.Buffer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public class AeronSubscriber extends BaseSubscriber<Buffer>
		implements ReactiveState.ActiveUpstream, ReactiveState.Upstream, ReactiveState.FeedbackLoop {

	private final Logger logger;

	private final AtomicBoolean alive = new AtomicBoolean(true);

	private volatile boolean terminated = false;

	private final AeronInfra aeronInfra;

	private final ServiceMessageHandler serviceMessageHandler;

	private final ServiceMessagePoller serviceMessagePoller;

	private final RingBufferProcessor<Buffer> processor;

	public static AeronSubscriber create(Context context) {
		context.validate();
		return new AeronSubscriber(context, false);
	}

	public static AeronSubscriber share(Context context) {
		context.validate();
		return new AeronSubscriber(context, true);
	}

	public AeronSubscriber(Context context,
						   Logger logger,
						   boolean multiPublishers,
						   Consumer<Void> shutdownTask) {

		this.logger = createLogger(logger);

		if (shutdownTask == null) {
			shutdownTask = new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					shutdown();
				}
			};
		}

		this.aeronInfra = context.createAeronInfra(this.logger);
		this.processor = createRingBufferProcessor(context, multiPublishers);
		boolean isMulticast = AeronUtils.isMulticastCommunication(context);
		if (isMulticast) {
			this.serviceMessageHandler = new MulticastServiceMessageHandler(processor, aeronInfra, context,
					shutdownTask);
		} else {
			this.serviceMessageHandler = new UnicastServiceMessageHandler(processor, aeronInfra, context,
					shutdownTask);
		}
		this.serviceMessagePoller = createServiceMessagePoller(context, aeronInfra, logger,
				serviceMessageHandler);

		//TODO: Do not start from constructor
		serviceMessageHandler.start();
		serviceMessagePoller.start();

		if (logger.isDebugEnabled()) {
			logger.debug("Initialized in {} sending mode", isMulticast ? "multicast" : "unicast");
		}
	}

	protected Logger createLogger(Logger parentLogger) {
		return parentLogger != Logger.getLogger(this.getClass()) ?
				Logger.getLogger(parentLogger.getName() + ".subscriber") : parentLogger;
	}

	public AeronSubscriber(Context context, boolean multiPublishers) {
		this(context,
				Logger.getLogger(AeronSubscriber.class),
				multiPublishers,
				null);
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

	private ServiceMessagePoller createServiceMessagePoller(Context context, AeronInfra aeronInfra, Logger logger,
															ServiceMessageHandler serviceMessageHandler) {
		return new ServiceMessagePoller(logger, context, aeronInfra, serviceMessageHandler);
	}

	private RingBufferProcessor<Buffer> createRingBufferProcessor(Context context, boolean multiPublishers) {
		String name = context.name() + "-signal-sender";
		return multiPublishers ?
				RingBufferProcessor.<Buffer>share(name, context.ringBufferSize(), context.autoCancel()) :
				RingBufferProcessor.<Buffer>create(name, context.ringBufferSize(), context.autoCancel());
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
