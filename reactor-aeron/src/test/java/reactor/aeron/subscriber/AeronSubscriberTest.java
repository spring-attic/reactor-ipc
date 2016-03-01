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
import reactor.aeron.Context;
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.ThreadSnapshot;

import java.time.Duration;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronSubscriberTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT);

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"global"}, TIMEOUT.toMillis()));
	}

	@Test
	public void testShutdown() {
		AeronSubscriber subscriber = AeronSubscriber.create(Context.create()
				.name("publisher")
				.senderChannel(AeronTestUtils.availableLocalhostChannel()));

		subscriber.shutdown();
	}

}