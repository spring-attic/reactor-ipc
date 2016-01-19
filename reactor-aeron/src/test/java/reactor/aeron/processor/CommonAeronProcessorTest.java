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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.aeron.Context;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.publisher.FluxProcessor;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.rx.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public abstract class CommonAeronProcessorTest {

	protected static final int TIMEOUT_SECS = 5;

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTeardown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT_SECS);

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash", "global"},
				TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	protected Context createContext() {
		return new Context()
				.autoCancel(false)
				.publicationRetryMillis(1000)
				.errorConsumer(Throwable::printStackTrace);
	}

	protected DataTestSubscriber<String> createTestSubscriber() {
		return DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
	}


	@Test
	public void testNextSignalIsReceived() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());
		DataTestSubscriber<String> subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);
		subscriber.request(4);

		Stream.just(Buffer.wrap("Live"),
				Buffer.wrap("Hard"),
				Buffer.wrap("Die"),
				Buffer.wrap("Harder"),
				Buffer.wrap("Extra")).subscribe(processor);

		subscriber.assertNextSignals("Live", "Hard", "Die", "Harder");

		subscriber.request(1);

		subscriber.assertNextSignals("Extra");

		//FIXME: Remove this work-around for bounded Stream.just not sending Complete signal
		subscriber.request(1);
	}

	@Test
	public void testCompleteSignalIsReceived() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());
		Stream.just(
				Buffer.wrap("One"),
				Buffer.wrap("Two"),
				Buffer.wrap("Three"))
				.subscribe(processor);

		DataTestSubscriber<String> subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);

		subscriber.request(1);
		subscriber.assertNextSignals("One");

		subscriber.request(1);
		subscriber.assertNextSignals("Two");

		subscriber.request(1);
		subscriber.assertNextSignals("Three");

		//FIXME: Remove this work-around for bounded Stream.just not sending Complete signal
		subscriber.request(1);

		subscriber.assertCompleteReceived();
	}

	@Test
	@Ignore
	public void testCompleteShutdownsProcessorWithNoSubscribers() {
		AeronProcessor processor = AeronProcessor.create(createContext());

		Publisher<Buffer> publisher = Subscriber::onComplete;

		publisher.subscribe(processor);
	}

	@Test
	public void testWorksWithTwoSubscribersViaEmitter() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());
		Stream.just(Buffer.wrap("Live"),
				Buffer.wrap("Hard"),
				Buffer.wrap("Die"),
				Buffer.wrap("Harder")).subscribe(processor);

		FluxProcessor<Buffer, Buffer> emitter = Processors.emitter();
		processor.subscribe(emitter);

		DataTestSubscriber<String> subscriber1 = createTestSubscriber();
		IO.bufferToString(emitter).subscribe(subscriber1);

		DataTestSubscriber<String> subscriber2 = createTestSubscriber();
		IO.bufferToString(emitter).subscribe(subscriber2);

		subscriber1.requestUnboundedWithTimeout();
		subscriber2.requestUnboundedWithTimeout();


		subscriber1.assertNextSignals("Live", "Hard", "Die", "Harder");
		subscriber2.assertNextSignals("Live", "Hard", "Die", "Harder");
		subscriber1.assertCompleteReceived();
		subscriber2.assertCompleteReceived();
	}

	@Test
	public void testClientReceivesException() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		// as error is delivered on a different channelId compared to signal
		// its delivery could shutdown the processor before the processor subscriber
		// receives signal
		Stream.concat(Stream.just(Buffer.wrap("Item")),
				Stream.fail(new RuntimeException("Something went wrong")))
				.subscribe(processor);

		DataTestSubscriber<String> subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);
		subscriber.requestUnboundedWithTimeout();

		subscriber.assertErrorReceived();

		Throwable throwable = subscriber.getLastErrorSignal();
		assertThat(throwable.getMessage(), is("Something went wrong"));
	}

	@Test
	public void testExceptionWithNullMessageIsHandled() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		DataTestSubscriber<String> subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);
		subscriber.requestUnboundedWithTimeout();

		Stream<Buffer> sourceStream = Stream.fail(new RuntimeException());
		sourceStream.subscribe(processor);

		subscriber.assertErrorReceived();

		Throwable throwable = subscriber.getLastErrorSignal();
		assertThat(throwable.getMessage(), is(""));
	}

	@Test
	public void testCancelsUpstreamSubscriptionWhenLastSubscriptionIsCancelledAndAutoCancel()
			throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext().autoCancel(true));

		final CountDownLatch subscriptionCancelledLatch = new CountDownLatch(1);
		Publisher<Buffer> dataPublisher = new Publisher<Buffer>() {
			@Override
			public void subscribe(Subscriber<? super Buffer> subscriber) {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						System.out.println("Requested: " + n);
					}

					@Override
					public void cancel() {
						System.out.println("Upstream subscription cancelled");
						subscriptionCancelledLatch.countDown();
					}
				});
			}
		};
		dataPublisher.subscribe(processor);

		DataTestSubscriber<String> client = createTestSubscriber();

		IO.bufferToString(processor).subscribe(client);
		client.requestUnboundedWithTimeout();

		processor.onNext(Buffer.wrap("Hello"));

		client.assertNextSignals("Hello");
		client.cancelSubscription();

		assertTrue("Subscription wasn't cancelled",
				subscriptionCancelledLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
	}

}
