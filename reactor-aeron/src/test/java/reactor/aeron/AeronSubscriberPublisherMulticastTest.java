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
import reactor.io.IO;
import reactor.io.net.tcp.support.SocketUtils;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberPublisherMulticastTest extends CommonSubscriberPublisherTest {

	private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

	@Override
	protected Context createContext(String name) {
		return new Context().name(name)
		                    .senderChannel(CHANNEL)
		                    .receiverChannel(CHANNEL);
	}

	@Test
	public void testSubscriptionCancellationDoesNotShutdownPublisherWhenNoAutocancel() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));
		Publishers.from(createBuffers(256))
		          .subscribe(subscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext("publisher").autoCancel(false));
		TestSubscriber<String> client = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		IO.bufferToString(publisher).subscribe(client);

		client.request(3);

		client.assertNumNextSignalsReceived(3);

		client.cancel();

		Thread.sleep(3000);

		DataTestSubscriber<String> client2 = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		IO.bufferToString(publisher)
		  .subscribe(client2);

		Thread.sleep(1000);

		client2.request(6);

		client2.assertNumNextSignalsReceived(6);

		client2.cancel();

		subscriber.shutdown();
		publisher.shutdown();

		TestSubscriber.waitFor(TIMEOUT_SECS, "Subscriber wasn't terminated", subscriber::isTerminated);
		TestSubscriber.waitFor(TIMEOUT_SECS, "Publisher wasn't terminated", publisher::isTerminated);
	}

}