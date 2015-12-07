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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.TestAeronInfra;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.subscriber.test.TestSubscriber;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronPublisherTest {

	private static final int TIMEOUT_SECS = 5;

	private ThreadSnapshot threadSnapshot;

	private TestAeronInfra aeronInfra;

	private Context context;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();

		aeronInfra = new TestAeronInfra();

		context = new Context() {
			@Override
			public AeronInfra createAeronInfra(Logger logger) {
				return aeronInfra;
			}
		};
		context.name("test");
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT_SECS);

		assertTrue(threadSnapshot.takeAndCompare(new String[] { "global-"}, TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	@Test
	public void testShutdown() {
		AeronPublisher publisher = AeronPublisher.create(context);

		TestSubscriber subscriber = TestSubscriber.createWithTimeoutSecs(1);
		publisher.subscribe(subscriber);

		publisher.shutdown();
	}

	@Test
	public void testShouldShutdownAfterMaxHeartbeatPublicationFailures() throws InterruptedException {
		aeronInfra.setShouldFailClaim(true);

		context.name("test")
			.heartbeatIntervalMillis(500)
			.publicationLingerMillis(0);

		AeronPublisher publisher = AeronPublisher.create(context);

		TestSubscriber subscriber = TestSubscriber.createWithTimeoutSecs(1);
		publisher.subscribe(subscriber);

		TestSubscriber.waitFor(2, "publisher didn't terminate due to heartbeat loss", publisher::isTerminated);
	}

}