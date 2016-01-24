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

package reactor.aeron;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.SignalPublicationFailedException;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.io.buffer.Buffer;
import reactor.rx.Stream;

import static org.junit.Assert.*;

/**
 * @author Anatoly Kadyshev
 */
public abstract class CommonSubscriberPublisherTest {

	public static final int TIMEOUT_SECS = 5;

	final String senderChannel = AeronTestUtils.availableLocalhostChannel();

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		assertTrue(threadSnapshot.takeAndCompare(new String[]{"hash", "global"},
				TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	protected abstract Context createContext(String name);

	protected List<Buffer> createBuffers(int n) {
		List<Buffer> items = new ArrayList<>(n);
		for (int i = 1; i <= n; i++) {
			items.add(Buffer.wrap("" + i));
		}
		return items;
	}

	@Test
	public void testNextSignalIsReceivedByPublisher() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));

		Stream.just(Buffer.wrap("One"), Buffer.wrap("Two"), Buffer.wrap("Three"))
		       .subscribe(subscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher"));

		DataTestSubscriber<String> clientSubscriber = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher).subscribe(clientSubscriber);

		clientSubscriber.requestUnboundedWithTimeout();

		clientSubscriber.assertNextSignalsEqual("One", "Two", "Three");
		clientSubscriber.assertCompleteReceived();
	}

	@Test
	public void testErrorShutsDownAeronPublisherAndSubscriber() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));
		AeronPublisher publisher = AeronPublisher.create(createContext("publisher"));

		DataTestSubscriber<String> clientSubscriber = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher).subscribe(clientSubscriber);

		clientSubscriber.requestUnboundedWithTimeout();

		Stream.<Buffer>fail(new RuntimeException("Something went wrong")).subscribe(subscriber);

		clientSubscriber.assertErrorReceived();
	}

	@Test
	public void testFailedOnNextSignalPublicationIsReported() throws InterruptedException {
		final CountDownLatch gotErrorLatch = new CountDownLatch(1);
		final AtomicReference<Throwable> error = new AtomicReference<>();
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber").errorConsumer(th -> {
			gotErrorLatch.countDown();
			error.set(th);
		}));

		final byte[] bytes = new byte[2048];
		Stream.range(1, 100).map(i -> Buffer.wrap(bytes)).subscribe(subscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(false));

		CountDownLatch onNextLatch = new CountDownLatch(1);
		publisher.subscribe(new BaseSubscriber<Buffer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Buffer buffer) {
				try {
					onNextLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});

		assertTrue(gotErrorLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));
		onNextLatch.countDown();

		assertThat(error.get(), Matchers.instanceOf(SignalPublicationFailedException.class));
	}

	static class TestPublisher implements Publisher<String> {

		private Subscriber<? super String> subscriber;

		private final CountDownLatch cancelledLatch = new CountDownLatch(1);

		@Override
		public void subscribe(Subscriber<? super String> s) {
			subscriber = s;
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					subscriber.onNext("" + n);
				}

				@Override
				public void cancel() {
					cancelledLatch.countDown();
				}
			});
		}

		public boolean awaitCancelled(int timeoutSecs) throws InterruptedException {
			return cancelledLatch.await(timeoutSecs, TimeUnit.SECONDS);
		}

	}

	@Test
	public void testUpstreamSubscriptionIsCancelledWhenAutoCancel() throws InterruptedException {
		TestPublisher valuePublisher = new TestPublisher();

		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber").autoCancel(true));
		Buffer.stringToBuffer(valuePublisher).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(true));
		DataTestSubscriber<String> client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher).subscribe(client);

		client.requestWithTimeout(1);
		client.cancel();


		assertTrue(valuePublisher.awaitCancelled(TIMEOUT_SECS));
	}

	@Test
	public void testUpstreamSubscriptionIsNotCancelledWhenNoAutoCancel() throws InterruptedException {
		TestPublisher valuePublisher = new TestPublisher();

		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber").autoCancel(false));
		Buffer.stringToBuffer(valuePublisher).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(false));
		DataTestSubscriber<String> client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher).subscribe(client);

		client.requestWithTimeout(1);
		client.cancel();


		assertFalse(valuePublisher.awaitCancelled(2));

		publisher.shutdown();
		aeronSubscriber.shutdown();
	}

}
