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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.core.util.ReactiveStateUtils;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberPublisherUnicastTest extends CommonSubscriberPublisherTest {

	@Override
	protected Context createContext(String name) {
		return new Context().name(name)
				.senderPort(senderPort);
	}

	@Test
	public void testNextSignalIsReceivedByTwoPublishers() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher1 = AeronPublisher.create(createContext("publisher1"));

		AeronPublisher publisher2 = AeronPublisher.create(
				createContext("publisher2")
						.receiverPort(SocketUtils.findAvailableUdpPort()));

		DataTestSubscriber<String>client1 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher1).subscribe(client1);


		client1.request(3);

		client1.assertNextSignalsEqual("1", "2", "3");

		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());
		System.out.println(ReactiveStateUtils.scan(client1).toString());



		DataTestSubscriber<String>client2 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher2).subscribe(client2);


		client2.request(6);

		client2.assertNextSignalsEqual("1", "2", "3", "4", "5", "6");

		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());
		System.out.println(ReactiveStateUtils.scan(client2).toString());


		//TODO: A temp work-around
		client2.request(1);

		client2.assertCompleteReceived();

		System.out.println(ReactiveStateUtils.scan(client1).toString());

		client1.request(3);
		client1.assertNextSignalsEqual("4", "5", "6");
		System.out.println(ReactiveStateUtils.scan(aeronSubscriber).toString());

		//TODO: A temp work-around
		client1.request(1);

		client1.assertCompleteReceived();
	}

	@Test
	public void testSubscriptionCancellationDoesNotShutdownPublisherWhenNoAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(false));
		DataTestSubscriber<String>client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);
		client.assertNextSignalsEqual("1", "2", "3");

		client.cancel();

		Thread.sleep(2000);


		DataTestSubscriber<String> client2 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher).subscribe(client2);

		Thread.sleep(1000);

		client2.request(6);
		client2.assertNextSignalsEqual("1", "2", "3", "4", "5", "6");

		// A temporary workaround
		client2.request(1);
		client2.assertCompleteReceived();
	}


	@Test
	public void testSubscriptionCancellationShutdownsPublisherWhenAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext("subscriber"));

		Flux.fromIterable(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(true));
		DataTestSubscriber<String>client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);

		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);
		client.assertNextSignalsEqual("1", "2", "3");

		client.cancel();

		TestSubscriber.waitFor(TIMEOUT_SECS, "AeronPublisher should be dead", () -> !publisher.alive());

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

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(true));
		HangingOnCompleteSubscriber client = new HangingOnCompleteSubscriber();
		Buffer.bufferToString(publisher).subscribe(client);

		assertTrue(client.completeReceivedLatch.await(TIMEOUT_SECS, TimeUnit.SECONDS));

		AeronPublisher publisher2 = AeronPublisher.create(
				createContext("publisher2").autoCancel(true).streamId(10).errorStreamId(11));

		TestSubscriber<String> client2 = TestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		Buffer.bufferToString(publisher2).subscribe(client2);

		client2.request(3 + 1);

		client2.assertNumNextSignalsReceived(3);
		client2.assertCompleteReceived();

		client.canReturnLatch.countDown();
	}

}