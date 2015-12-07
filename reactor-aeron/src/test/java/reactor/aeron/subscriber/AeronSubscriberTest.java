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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.Timers;
import reactor.aeron.Context;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.ThreadSnapshot;
import reactor.io.net.tcp.support.SocketUtils;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberTest {

	private static final int TIMEOUT_SECS = 5;

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT_SECS);

		Timers.unregisterGlobal();

		assertTrue(threadSnapshot.takeAndCompare(TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	@Test
	public void testShutdown() {
		final int senderPort = SocketUtils.findAvailableTcpPort();

		AeronSubscriber subscriber = AeronSubscriber.create(new Context()
				.name("publisher")
				.publicationLingerMillis(2000)
				.senderPort(senderPort));

		subscriber.shutdown();
	}

}