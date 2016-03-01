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

import org.junit.Test;
import reactor.aeron.Context;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.utils.AeronTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.test.TestSubscriber;
import reactor.io.buffer.Buffer;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberPublisherMulticastTest extends CommonSubscriberPublisherTest {

	private String CHANNEL = AeronTestUtils.availableLocalhostChannel();

	@Override
	protected Context createContext(String name) {
		return Context.create().name(name)
		                    .senderChannel(CHANNEL)
		                    .receiverChannel(CHANNEL);
	}

	@Test
	public void testSubscriptionCancellationDoesNotShutdownPublisherWhenNoAutocancel() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext("subscriber"));
		Flux.fromIterable(createBuffers(256))
		    .subscribe(subscriber);

		AeronFlux publisher = new AeronFlux(createContext("publisher").autoCancel(false));
		TestSubscriber<String> client = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher).subscribe(client);

		client.request(3);

		client.awaitAndAssertValueCount(3);

		client.cancel();

		Thread.sleep(3000);

		TestSubscriber<String> client2 = new TestSubscriber<String>(0);
		Buffer.bufferToString(publisher)
		  .subscribe(client2);

		Thread.sleep(1000);

		client2.request(6);

		client2.awaitAndAssertValueCount(6);

		client2.cancel();

		subscriber.shutdown();
		publisher.shutdown();

		TestSubscriber.await(TIMEOUT, "Subscriber wasn't terminated", subscriber::isTerminated);
		TestSubscriber.await(TIMEOUT, "Publisher wasn't terminated", publisher::isTerminated);
	}

}