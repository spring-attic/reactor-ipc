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

import org.junit.Test;
import reactor.Publishers;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.core.support.ReactiveStateUtils;
import reactor.io.net.tcp.support.SocketUtils;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberPublisherUnicastTest extends CommonSubscriberPublisherTest {

	@Override
	protected Context createContext() {
		return new Context().name("test")
				.senderPort(senderPort);
	}

	@Test
	public void testNextSignalIsReceivedByTwoPublishers() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext());

		Publishers.from(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher1 = AeronPublisher.create(createContext());

		AeronPublisher publisher2 = AeronPublisher.create(
				createContext()
						.receiverPort(SocketUtils.findAvailableTcpPort()));

		DataTestSubscriber client1 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		publisher1.subscribe(client1);


		// Waiting till Aeron publications are created to avoid switching to async sender
		Thread.sleep(2000);
		System.out.println(ReactiveStateUtils.scan(client1).toString());


		client1.request(3);


		client1.assertNextSignals("1", "2", "3");


		DataTestSubscriber client2 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		publisher2.subscribe(client2);

		// Waiting till Aeron publications are created to avoid switching to async sender
		Thread.sleep(2000);
		System.out.println(ReactiveStateUtils.scan(client2).toString());

		client2.request(6);

		client2.assertNextSignals("1", "2", "3", "4", "5", "6");

		//TODO: A temp work-around
		client2.request(1);

		client2.assertCompleteReceived();

		System.out.println(ReactiveStateUtils.scan(client1).toString());

		client1.request(3);
		client1.assertNextSignals("4", "5", "6");

		//TODO: A temp work-around
		client1.request(1);

		client1.assertCompleteReceived();
	}

	@Test
	public void testSubscriptionCancellationDoesNotShutdownPublisherWhenNoAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext());

		Publishers.from(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext().autoCancel(false));
		DataTestSubscriber client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);

		publisher.subscribe(client);

		client.request(3);
		client.assertNextSignals("1", "2", "3");

		client.cancel();

		Thread.sleep(2000);


		DataTestSubscriber client2 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		publisher.subscribe(client2);

		Thread.sleep(1000);

		client2.request(6);
		client2.assertNextSignals("1", "2", "3", "4", "5", "6");

		// A temporary workaround
		client2.request(1);
		client2.assertCompleteReceived();
	}


	@Test
	public void testSubscriptionCancellationShutdownsPublisherWhenAutocancel() throws InterruptedException {
		AeronSubscriber aeronSubscriber = AeronSubscriber.create(createContext());

		Publishers.from(createBuffers(6)).subscribe(aeronSubscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext().autoCancel(true));
		DataTestSubscriber client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);

		publisher.subscribe(client);

		client.request(3);
		client.assertNextSignals("1", "2", "3");

		client.cancel();

		TestSubscriber.waitFor(TIMEOUT_SECS, "AeronPublisher should be dead", () -> !publisher.alive());

		aeronSubscriber.shutdown();
	}

}