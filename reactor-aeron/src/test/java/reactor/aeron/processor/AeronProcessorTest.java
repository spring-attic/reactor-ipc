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
package reactor.aeron.processor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.aeron.Context;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Stream;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorTest {

	protected final int TIMEOUT_SECS = 5;

	private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

	private AeronProcessor processor;

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();
	}

	@After
	public void doTearDown() throws InterruptedException {
		if (processor != null) {
			processor.shutdown();

			TestSubscriber.waitFor(TIMEOUT_SECS, "Processor didn't terminate within timeout interval",
					() -> processor.isTerminated());
		}

		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT_SECS);

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash", "global"},
				TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	protected DataTestSubscriber<String>createTestSubscriber() {
		return DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
	}

	@Test
	public void testProcessorWorksWithExternalMediaDriver() throws InterruptedException {
		MediaDriver.Context context = new MediaDriver.Context();
		final MediaDriver mediaDriver = MediaDriver.launch(context);
		Aeron.Context ctx = new Aeron.Context();
		ctx.aeronDirectoryName(mediaDriver.aeronDirectoryName());
		final Aeron aeron = Aeron.connect(ctx);
		try {
			AeronProcessor processor = AeronProcessor.create(new Context()
					.name("processor")
					.autoCancel(false)
					.senderChannel(CHANNEL)
					.receiverChannel(CHANNEL)
					.aeron(aeron));

			Stream.just(
					Buffer.wrap("Live"))
					.subscribe(processor);

			DataTestSubscriber<String>subscriber = createTestSubscriber();
			IO.bufferToString(processor).subscribe(subscriber);
			subscriber.requestUnboundedWithTimeout();

			subscriber.assertNextSignals("Live");
			subscriber.assertCompleteReceived();

			TestSubscriber.waitFor(TIMEOUT_SECS, "Processor didn't terminate within timeout interval",
					processor::isTerminated);
		} finally {
			aeron.close();

			mediaDriver.close();

			try {
				System.out.println("Cleaning up media driver files: " + context.aeronDirectoryName());
				context.deleteAeronDirectory();
			} catch (Exception e) {
			}
		}
	}

	@Test
	public void testCreate() throws InterruptedException {
		processor = AeronProcessor.create(createAeronContext());

		Stream.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		DataTestSubscriber<String>subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);
		subscriber.requestUnboundedWithTimeout();

		subscriber.assertNextSignals("Live");
		subscriber.assertCompleteReceived();
	}

	@Test
	public void testShare() throws InterruptedException {
		processor = AeronProcessor.share(createAeronContext());

		Stream.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		DataTestSubscriber<String>subscriber = createTestSubscriber();
		IO.bufferToString(processor).subscribe(subscriber);
		subscriber.requestUnboundedWithTimeout();

		subscriber.assertNextSignals("Live");
		subscriber.assertCompleteReceived();
	}

	protected Context createAeronContext() {
		return new Context().name("multicast")
				.senderChannel(CHANNEL)
				.receiverChannel(CHANNEL);
	}

}