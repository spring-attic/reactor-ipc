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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.utils.AeronUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.TopicProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Operators;
import reactor.ipc.buffer.Buffer;
import reactor.core.Exceptions;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;

/**
 * Is not actually a test but a proof of concept of threading for {@link AeronSubscriber}
 *
 * @author Anatoly Kadyshev
 */
@Ignore
public class SubscriberThreadingPOCTest {

	class SignalPollerForPOC implements Runnable {

		private final FluxProcessor<Buffer, Buffer> sessionProcessor;

		private final ConcurrentLinkedQueue<ServiceRequest> requests = new ConcurrentLinkedQueue<>();

		private volatile boolean running;

		private final CountDownLatch started = new CountDownLatch(1);

		class ServiceRequest {

			static final int REQUEST = 1;

			static final int CANCEL = 2;

			final int requestType;

			final long n;

			ServiceRequest(int requestType, long n) {
				this.requestType = requestType;
				this.n = n;
			}

		}

		final class SenderSubscriber implements Subscriber<Buffer> {

			private Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				if(Operators.validate(subscription, s)) {
					this.subscription = s;
					log(this.getClass()
					        .getSimpleName() + ".onSubscribe: " + s);

					started.countDown();
				}
			}

			@Override
			public void onNext(Buffer buffer) {
				if (buffer == null) {
					throw Exceptions.argumentIsNullException();
				}

				log(this.getClass().getSimpleName() + ".onNext: " + buffer.asString());
			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onComplete() {
				log(this.getClass().getSimpleName() + ".onComplete");
			}
		}

		SignalPollerForPOC(FluxProcessor<Buffer, Buffer> sessionProcessor) {
			this.sessionProcessor = sessionProcessor;
		}

		public void request(long n) {
			requests.add(new ServiceRequest(ServiceRequest.REQUEST, n));
		}

		public void cancel() {
			requests.add(new ServiceRequest(ServiceRequest.CANCEL, -1));
		}

		@Override
		public void run() {
			this.running = true;

			BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

			SenderSubscriber subscriber;
			subscriber = new SenderSubscriber();
			sessionProcessor.subscribe(subscriber);

			while(running) {
				ServiceRequest request = requests.poll();
				if (request != null) {

					if (request.requestType == ServiceRequest.REQUEST) {
						subscriber.subscription.request(request.n);
					} else if (request.requestType == ServiceRequest.CANCEL) {
						subscriber.subscription.cancel();
					} else {
						throw new IllegalStateException("Unknown request type of: " + request.requestType);
					}

					idleStrategy.idle(1);
				} else {
					idleStrategy.idle(0);
				}
			}
		}

		public void shutdown() {
			this.running = false;
		}

		public void awaitStarted() throws InterruptedException {
			started.await(5, TimeUnit.SECONDS);
		}
	}

	class SyncPublisher implements Publisher<Buffer> {

		private volatile long totalSignals;

		public SyncPublisher(long totalSignals) {
			this.totalSignals = totalSignals;
		}

		@Override
		public void subscribe(Subscriber<? super Buffer> subscriber) {
			subscriber.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					log("requested: " + n);
					for (int i = 0; i < n; i++) {
						if (--totalSignals == 0) {
							subscriber.onComplete();
							break;
						}
						subscriber.onNext(Buffer.wrap("" + i));
					}
				}

				@Override
				public void cancel() {
					log("upstream subscription cancelled");
				}
			});

			if (totalSignals == 0) {
				subscriber.onComplete();
			}
		}
	}

	@Test
	public void test() throws InterruptedException {
		Publisher<Buffer> dataPublisher = new SyncPublisher(32);
		TopicProcessor<Buffer> processor = TopicProcessor.create("ringbuffer-sender", 8);
		dataPublisher.subscribe(processor);

		SignalPollerForPOC signalPoller = new SignalPollerForPOC(processor);
		Thread signalPollerThread = new Thread(signalPoller, "signal-poller");
		signalPollerThread.start();
		signalPoller.awaitStarted();

		signalPoller.request(1);

		Thread.sleep(1000);

		signalPoller.cancel();

		Thread.sleep(1000);
	}

	@Test
	public void testCompletedPublisher() throws InterruptedException {
		Publisher<Buffer> dataPublisher = new SyncPublisher(4);
		TopicProcessor<Buffer> processor = TopicProcessor.create("ringbuffer-sender", 8);
		dataPublisher.subscribe(processor);

		SignalPollerForPOC signalPoller = new SignalPollerForPOC(processor);
		Thread signalPollerThread = new Thread(signalPoller, "signal-poller");
		signalPollerThread.start();
		signalPoller.awaitStarted();

		signalPoller.request(1);

		Thread.sleep(1000);

		signalPoller.cancel();

		Thread.sleep(1000);
	}

	@Test
	public void testSubscribeOn() throws InterruptedException {
		Publisher<Buffer> dataPublisher = new SyncPublisher(32);

		Scheduler group = Schedulers.parallel();
		TopicProcessor<Buffer> processor = TopicProcessor.create("ringbuffer-sender", 8);
		Flux.from(dataPublisher).subscribeOn(group).subscribe(processor);

		SignalPollerForPOC signalPoller = new SignalPollerForPOC(processor);
		Thread signalPollerThread = new Thread(signalPoller, "signal-poller");
		signalPollerThread.start();
		signalPoller.awaitStarted();

		signalPoller.request(1);

		Thread.sleep(1000);

		signalPoller.cancel();

		Thread.sleep(1000);
	}

	private void log(String msg) {
		System.out.println("[" + Thread.currentThread().getName() + "]" + " - " + msg);
	}

}
