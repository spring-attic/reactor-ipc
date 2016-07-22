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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.ThreadSnapshot;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.test.TestSubscriber;
import reactor.ipc.buffer.Buffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public abstract class CommonAeronProcessorTest {

	protected static final Duration TIMEOUT = Duration.ofSeconds(5);

	private ThreadSnapshot threadSnapshot;

	protected String CHANNEL = AeronTestUtils.availableLocalhostChannel();

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTeardown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT);

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash", "global"},
				TIMEOUT.toMillis()));
	}

	protected Context createContext() {
		return Context.create()
				.senderChannel(CHANNEL)
				.errorConsumer(Throwable::printStackTrace);
	}


	@Test
	public void testNextSignalIsReceived() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());
		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);
		subscriber.request(4);

		Flux.just(Buffer.wrap("Live"),
				Buffer.wrap("Hard"),
				Buffer.wrap("Die"),
				Buffer.wrap("Harder"),
				Buffer.wrap("Extra")).subscribe(processor);

		subscriber.awaitAndAssertNextValues("Live", "Hard", "Die", "Harder");

		subscriber.request(1);

		subscriber.awaitAndAssertNextValues("Extra");
		subscriber.await();
	}

	@Test
	public void testCompleteSignalIsReceived() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());
		Flux.just(
				Buffer.wrap("One"),
				Buffer.wrap("Two"),
				Buffer.wrap("Three"))
				.subscribe(processor);

		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);

		subscriber.request(1);
		subscriber.awaitAndAssertNextValues("One");

		subscriber.request(1);
		subscriber.awaitAndAssertNextValues("Two");

		subscriber.request(1);
		subscriber.awaitAndAssertNextValues("Three").assertComplete();
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
		Flux.just(Buffer.wrap("Live"),
				Buffer.wrap("Hard"),
				Buffer.wrap("Die"),
				Buffer.wrap("Harder")).subscribe(processor);

		FluxProcessor<Buffer, Buffer> emitter = EmitterProcessor.create();
		processor.subscribe(emitter);

		TestSubscriber<String> subscriber1 = TestSubscriber.create();
		Buffer.bufferToString(emitter).subscribe(subscriber1);

		TestSubscriber<String> subscriber2 = TestSubscriber.create();
		Buffer.bufferToString(emitter).subscribe(subscriber2);

		subscriber1.awaitAndAssertNextValues("Live", "Hard", "Die", "Harder").assertComplete();
		subscriber2.awaitAndAssertNextValues("Live", "Hard", "Die", "Harder").assertComplete();
	}

	@Test
	public void testClientReceivesException() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		// as error is delivered on a different channelId compared to signal
		// its delivery could shutdown the processor before the processor subscriber
		// receives signal
		Flux.concat(Flux.just(Buffer.wrap("Item")),
				Flux.error(new RuntimeException("Something went wrong")))
				.subscribe(processor);

		TestSubscriber<String> subscriber = TestSubscriber.create();
		Buffer.bufferToString(processor).subscribe(subscriber);

		subscriber.await(TIMEOUT).assertErrorWith(t -> assertThat(t.getMessage(), is("Something went wrong")));
	}

	@Test
	public void testExceptionWithNullMessageIsHandled() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		TestSubscriber<String> subscriber = TestSubscriber.create();
		Buffer.bufferToString(processor).subscribe(subscriber);

		Flux<Buffer> sourceStream = Flux.error(new RuntimeException());
		sourceStream.subscribe(processor);

		subscriber.await(TIMEOUT).assertErrorWith(t -> assertThat(t.getMessage(), is("")));
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

		TestSubscriber<String> client = TestSubscriber.create();

		Buffer.bufferToString(processor).subscribe(client);

		processor.onNext(Buffer.wrap("Hello"));

		client.awaitAndAssertNextValues("Hello").cancel();

		assertTrue("Subscription wasn't cancelled",
				subscriptionCancelledLatch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
	}

	@Test
	public void testRemotePublisherReceivesCompleteBeforeProcessorIsShutdown() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		Flux.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);

		AeronFlux remotePublisher = new AeronFlux(createContext());
		TestSubscriber<String> remoteSubscriber = TestSubscriber.create(0);
		Buffer.bufferToString(remotePublisher).subscribe(remoteSubscriber);

		subscriber.request(1);
		remoteSubscriber.request(1);

		subscriber.awaitAndAssertNextValues("Live").assertComplete();
		remoteSubscriber.awaitAndAssertNextValues("Live").assertComplete();
	}

	@Test
	public void testRemotePublisherReceivesErrorBeforeProcessorIsShutdown() throws InterruptedException {
		AeronProcessor processor = AeronProcessor.create(createContext());

		Flux.<Buffer>error(new Exception("Oops!")).subscribe(processor);

		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);

		AeronFlux remotePublisher = new AeronFlux(createContext());
		TestSubscriber<String> remoteSubscriber = TestSubscriber.create(0);
		Buffer.bufferToString(remotePublisher).subscribe(remoteSubscriber);

		subscriber.request(1);
		remoteSubscriber.request(1);

		subscriber.await(TIMEOUT).assertError();
		remoteSubscriber.await(TIMEOUT).assertError();
	}

}
