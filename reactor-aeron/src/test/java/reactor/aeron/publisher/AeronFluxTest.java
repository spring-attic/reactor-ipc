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

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.aeron.Context;
import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.TestAeronInfra;
import reactor.aeron.utils.ThreadSnapshot;
import reactor.test.TestSubscriber;
import reactor.ipc.buffer.Buffer;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronFluxTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(5);

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
			public AeronInfra aeronInfra() {
				return aeronInfra;
			}
		};
		context.name("test");
		context.senderChannel("udp://localhost:12000");
		context.receiverChannel("udp://localhost:12001");
	}

	@After
	public void doTearDown() throws InterruptedException {
		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT);

		assertTrue(threadSnapshot.takeAndCompare(new String[] { "global-"}, TIMEOUT.toMillis()));
	}

	@Test
	public void testShutdown() {
		AeronFlux publisher = new AeronFlux(context);

		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(publisher).subscribe(subscriber);

		publisher.shutdown();
	}

	@Test
	public void testShouldShutdownAfterMaxHeartbeatPublicationFailures() throws InterruptedException {
		aeronInfra.setShouldFailClaim(true);

		context.name("test")
			.heartbeatIntervalMillis(500);

		AeronFlux publisher = new AeronFlux(context);

		TestSubscriber<String> subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(publisher).subscribe(subscriber);

		TestSubscriber.await(Duration.ofSeconds(2), "publisher didn't terminate due to heartbeat loss", publisher::isTerminated);
	}

}