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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.utils.AeronTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.test.TestSubscriber;
import reactor.core.util.ReactiveStateUtils;
import reactor.io.buffer.Buffer;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberPublisherUnicastTest extends CommonSubscriberPublisherTest {

	@Override
	protected Context createContext(String name) {
		return new Context().name(name)
				.senderChannel(senderChannel)
				.receiverChannel(AeronTestUtils.availableLocalhostChannel());
	}

	@Test
	public void testNextSignalIsReceivedByTwoPublishers() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronFlux publisher1 = new AeronFlux(createContext("publisher1"));

		TestSubscriber<String> client1 = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher1).subscribe(client1);


		client1.request(3);

		client1.awaitAndAssertValues("1", "2", "3");

		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());
		System.out.println(ReactiveStateUtils.scan(client1).toString());

		AeronFlux publisher2 = new AeronFlux(createContext("publisher2")
				.receiverChannel(AeronTestUtils.availableLocalhostChannel()));

		TestSubscriber<String> client2 = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher2).subscribe(client2);


		client2.request(6);

		client2.awaitAndAssertValues("1", "2", "3", "4", "5", "6").assertComplete();

		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());
		System.out.println(ReactiveStateUtils.scan(client2).toString());
		System.out.println(ReactiveStateUtils.scan(client1).toString());

		client1.request(3);

		client1.awaitAndAssertValues("4", "5", "6").assertComplete();

		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());
	}

	@Test
	public void testSubscriptionCancellationDoesNotShutdownPublisherWhenNoAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));
		TestSubscriber<String> client = new TestSubscriber<String>(0);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);
		client.awaitAndAssertValues("1", "2", "3");

		client.cancel();

		Thread.sleep(2000);


		TestSubscriber<String> client2 = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher).subscribe(client2);

		Thread.sleep(1000);

		client2.request(6);
		client2.awaitAndAssertValues("1", "2", "3", "4", "5", "6").assertComplete();
	}


	@Test
	public void testSubscriptionCancellationShutdownsPublisherWhenAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber").autoCancel(true));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(true));
		TestSubscriber<String> client = new TestSubscriber<String>(0);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);
		client.awaitAndAssertValues("1", "2", "3");

		client.cancel();

		TestSubscriber.await(TIMEOUT_SECS, "AeronFlux should be dead", () -> !publisher.alive());

		aeronSubscriber.shutdown();
	}

	private static class HangingOnCompleteSubscriber implements Subscriber<String> {

		CountDownLatch completeReceivedLatch = new CountDownLatch(1);

		CountDownLatch canReturnLatch = new CountDownLatch(1);

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Integer.MAX_VALUE);
		}

		@Override
		public void onNext(String s) {
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onComplete() {
			try {
				completeReceivedLatch.countDown();
				canReturnLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Test
	public void testPublisherCanConnectToATerminalButRunningSubscriber() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));
		Flux.fromIterable(createBuffers(3)).subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher"));
		HangingOnCompleteSubscriber client = new HangingOnCompleteSubscriber();
		Buffer.bufferToString(publisher).subscribe(client);

		assertTrue(client.completeReceivedLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));

		AeronFlux publisher2 = new AeronFlux(createContext("publisher2"));

		TestSubscriber<String> client2 = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher2).subscribe(client2);

		client2.request(3);

		client2.awaitAndAssertValueCount(3).assertComplete();

		client.canReturnLatch.countDown();
	}

	@Test
	public void testRequestAfterCompleteEventIsNoOp() {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(3)).subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));
		TestSubscriber<String> client = new TestSubscriber<>(0);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);

		client.awaitAndAssertValues("1", "2", "3");
		client.assertComplete();

		TestSubscriber.await(TIMEOUT_SECS, () -> "Publisher hasn't been terminated", publisher::isTerminated);

		client.request(1);
	}

	@Test
	public void testRequestAfterErrorEventIsNoOp() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.<Buffer>error(new RuntimeException("Oops!")).subscribe(aeronSubscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));
		TestSubscriber<String> client = new TestSubscriber<>(0);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(1);

		client.await(TIMEOUT_SECS).assertError();

		TestSubscriber.await(TIMEOUT_SECS, () -> "Publisher hasn't been terminated", publisher::isTerminated);

		client.request(1);
	}

}