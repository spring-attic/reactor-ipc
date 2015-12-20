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
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Streams;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public abstract class CommonSubscriberPublisherTest {

	public static final int TIMEOUT_SECS = 5;

	final int senderPort = SocketUtils.findAvailableUdpPort();

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

	protected abstract Context createContext();

	protected List<Buffer> createBuffers(int n) {
		List<Buffer> items = new ArrayList<>(n);
		for (int i = 1; i <= n; i++) {
			items.add(Buffer.wrap("" + i));
		}
		return items;
	}

	@Test
	public void testNextSignalIsReceivedByPublisher() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext());

		Streams.just(Buffer.wrap("One"), Buffer.wrap("Two"), Buffer.wrap("Three"))
		       .subscribe(subscriber);

		AeronPublisher publisher = AeronPublisher.create(createContext());

		DataTestSubscriber<String> clientSubscriber = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		IO.bufferToString(publisher).subscribe(clientSubscriber);

		clientSubscriber.requestUnboundedWithTimeout();

		clientSubscriber.assertNextSignals("One", "Two", "Three");
		clientSubscriber.assertCompleteReceived();
	}

	@Test
	public void testErrorShutsDownAeronPublisherAndSubscriber() throws InterruptedException {
		AeronSubscriber subscriber = AeronSubscriber.create(createContext());
		AeronPublisher publisher = AeronPublisher.create(createContext());

		DataTestSubscriber<String> clientSubscriber = DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
		IO.bufferToString(publisher).subscribe(clientSubscriber);

		clientSubscriber.requestUnboundedWithTimeout();

		Thread.sleep(1000);

		Streams.<Buffer, Throwable>fail(new RuntimeException("Something went wrong")).subscribe(subscriber);

		clientSubscriber.assertErrorReceived();
	}

}
