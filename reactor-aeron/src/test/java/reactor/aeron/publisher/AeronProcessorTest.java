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
import reactor.aeron.utils.AeronTestUtils;
import reactor.aeron.utils.ThreadSnapshot;
import reactor.core.publisher.Flux;
import reactor.test.TestSubscriber;
import reactor.ipc.buffer.Buffer;
import reactor.ipc.netty.util.SocketUtils;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;

import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class AeronProcessorTest {

	protected final Duration TIMEOUT = Duration.ofSeconds(5);

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

			TestSubscriber.await(TIMEOUT, "Processor didn't terminate within timeout interval",
					() -> processor.isTerminated());
		}

		AeronTestUtils.awaitMediaDriverIsTerminated(TIMEOUT);

		assertTrue(threadSnapshot.takeAndCompare(new String[] {"hash", "global"},
				TIMEOUT.toMillis()));
	}

	@Test
	public void testProcessorWorksWithExternalMediaDriver() throws InterruptedException {
		MediaDriver.Context context = new MediaDriver.Context();
		final MediaDriver mediaDriver = MediaDriver.launch(context);
		Aeron.Context ctx = new Aeron.Context();
		ctx.aeronDirectoryName(mediaDriver.aeronDirectoryName());
		final Aeron aeron = Aeron.connect(ctx);
		try {
			AeronProcessor processor = AeronProcessor.create(Context.create()
					.name("processor")
					.autoCancel(false)
					.senderChannel(CHANNEL)
					.receiverChannel(CHANNEL)
					.aeron(aeron));

			Flux.just(
					Buffer.wrap("Live"))
			    .subscribe(processor);

			TestSubscriber<String> subscriber = TestSubscriber.create(0);
			Buffer.bufferToString(processor).subscribe(subscriber);
			subscriber.request(1);

			subscriber.awaitAndAssertNextValues("Live").assertComplete();

			TestSubscriber.await(TIMEOUT, "Processor didn't terminate within timeout interval",
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

		Flux.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		TestSubscriber<String>subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);
		subscriber.request(1);

		subscriber.awaitAndAssertNextValues("Live").assertComplete();
	}

	@Test
	public void testShare() throws InterruptedException {
		processor = AeronProcessor.share(createAeronContext());

		Flux.just(
				Buffer.wrap("Live"))
				.subscribe(processor);

		TestSubscriber<String>subscriber = TestSubscriber.create(0);
		Buffer.bufferToString(processor).subscribe(subscriber);
		subscriber.request(1);

		subscriber.awaitAndAssertNextValues("Live").assertComplete();
	}

	protected Context createAeronContext() {
		return Context.create().name("multicast")
				.senderChannel(CHANNEL)
				.receiverChannel(CHANNEL);
	}

}