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

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.aeron.Context;
import reactor.aeron.support.AeronTestUtils;
import reactor.aeron.support.EmbeddedMediaDriverManager;
import reactor.aeron.support.ThreadSnapshot;
import reactor.core.subscriber.test.DataTestSubscriber;
import reactor.core.subscriber.test.TestSubscriber;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for communication between several AeronProcessor instances for multicast case. For the moment we don't support
 * multiple Aeron senders via multicast and therefore test is ignored. TODO: Remove the test or re-enable
 * @author Anatoly Kadyshev
 */
@Ignore
public class AeronProcessorMultipleInstancesTest {

	private static final int TIMEOUT_SECS = 5;

	private String CHANNEL = "udp://localhost:" + SocketUtils.findAvailableUdpPort();

	private ThreadSnapshot threadSnapshot;

	@Before
	public void doSetup() {
		threadSnapshot = new ThreadSnapshot().take();

		AeronTestUtils.setAeronEnvProps();

		assertThat(EmbeddedMediaDriverManager.getInstance()
		                                     .getCounter(), is(0));
	}

	@After
	public void doTeardown() throws InterruptedException {
		assertTrue(threadSnapshot.takeAndCompare(new String[]{"hash", "global"},
				TimeUnit.SECONDS.toMillis(TIMEOUT_SECS)));
	}

	@Test
	public void test_OtherProcessor_Error_Shutdowns_Mine() throws InterruptedException {
		AeronProcessor myProcessor = createProcessor("myProcessor");
		TestSubscriber<String> mySubscriber = createTestSubscriber();
		IO.bufferToString(myProcessor)
		  .subscribe(mySubscriber);
		mySubscriber.requestUnboundedWithTimeout();

		AeronProcessor otherProcessor = createProcessor("otherProcessor");
		TestSubscriber<String> otherSubscriber = createTestSubscriber();
		IO.bufferToString(otherProcessor)
		  .subscribe(otherSubscriber);
		otherSubscriber.requestUnboundedWithTimeout();

		otherProcessor.onError(new RuntimeException("Bah"));

		mySubscriber.assertErrorReceived();
		otherSubscriber.assertErrorReceived();

		TestSubscriber.waitFor(TIMEOUT_SECS, "otherProcessor is still alive", () -> !otherProcessor.alive());

		TestSubscriber.waitFor(TIMEOUT_SECS, "myProcessor is still alive", () -> !myProcessor.alive());

	}

	@Test
	public void test_OtherProcessor_Complete_DoesNotShutdown_Mine() throws InterruptedException {
		AeronProcessor myProcessor = createProcessor("myProcessor");
		TestSubscriber<String> mySubscriber = createTestSubscriber();
		IO.bufferToString(myProcessor)
		  .subscribe(mySubscriber);
		mySubscriber.requestUnboundedWithTimeout();

		AeronProcessor otherProcessor = createProcessor("otherProcessor");
		TestSubscriber<String> otherSubscriber = createTestSubscriber();
		IO.bufferToString(otherProcessor)
		  .subscribe(otherSubscriber);
		otherSubscriber.requestUnboundedWithTimeout();

		otherProcessor.onComplete();

		otherSubscriber.assertCompleteReceived();
		mySubscriber.assertNoCompleteReceived();

		myProcessor.onComplete();

		mySubscriber.assertCompleteReceived();
	}

	@Test
	public void testReceiverGetsNextSignals() throws InterruptedException {
		AeronProcessor myProcessor = createProcessor("myProcessor");
		Stream.just(Buffer.wrap("Live"))
		       .subscribe(myProcessor);

		AeronProcessor otherProcessor = createProcessor("otherProcessor");
		otherProcessor.onNext(Buffer.wrap("Glory"));

		DataTestSubscriber<String> mySubscriber = createTestSubscriber();
		IO.bufferToString(myProcessor)
		  .subscribe(mySubscriber);
		mySubscriber.requestUnboundedWithTimeout();

		mySubscriber.assertNextSignals("Live", "Glory");
		mySubscriber.assertCompleteReceived();

		otherProcessor.onComplete();
	}

	@Test
	public void testSingleSenderTwoReceivers() throws InterruptedException {
		AeronProcessor server = createProcessor("server");
		Stream.just(Buffer.wrap("One"), Buffer.wrap("Two"), Buffer.wrap("Three"))
		       .subscribe(server);

		AeronProcessor client1 = createProcessor("client-1");
		DataTestSubscriber<String> subscriber1 = createTestSubscriber();
		IO.bufferToString(client1).subscribe(subscriber1);

		AeronProcessor client2 = createProcessor("client-2");
		DataTestSubscriber<String> subscriber2 = createTestSubscriber();
		IO.bufferToString(client2).subscribe(subscriber2);

		subscriber1.request(1);
		subscriber1.assertNextSignals("One");
		subscriber2.assertNumNextSignalsReceived(0);

		subscriber1.request(1);
		subscriber1.assertNextSignals("One", "Two");
		subscriber2.assertNumNextSignalsReceived(0);

		subscriber2.request(1);
		subscriber1.assertNextSignals("One", "Two");
		subscriber2.assertNextSignals("One");

		subscriber1.request(1);
		subscriber2.request(2);
		subscriber1.assertNextSignals("One", "Two", "Three");
		subscriber2.assertNextSignals("One", "Two", "Three");

		client1.onComplete();
		client2.onComplete();

		subscriber1.assertCompleteReceived();
		subscriber2.assertCompleteReceived();
	}

	@Test
	public void testSingleSenderSendsErrorToTwoReceivers() throws InterruptedException {
		AeronProcessor server = createProcessor("server");
		Stream.concat(Stream.just(Buffer.wrap("One"), Buffer.wrap("Two"), Buffer.wrap("Three")),
				Stream.fail(new RuntimeException("Something went wrong")))
		       .subscribe(server);

		AeronProcessor client1 = createProcessor("client-1");
		DataTestSubscriber<String> subscriber1 = createTestSubscriber();
		IO.bufferToString(client1).subscribe(subscriber1);

		AeronProcessor client2 = createProcessor("client-2");
		TestSubscriber<String> subscriber2 = createTestSubscriber();
		IO.bufferToString(client2).subscribe(subscriber2);

		subscriber1.request(1);
		subscriber1.assertNextSignals("One");
		subscriber2.assertNumNextSignalsReceived(0);

		subscriber1.request(1);

		subscriber1.assertNextSignals("One", "Two");
		subscriber2.assertNumNextSignalsReceived(0);

		subscriber1.request(1);

		subscriber1.assertErrorReceived();
		subscriber2.assertErrorReceived();
	}

	private AeronProcessor createProcessor(String name) {
		return AeronProcessor.share(new Context().name(name)
		                                         .autoCancel(false)
		                                         .senderChannel(CHANNEL)
		                                         .receiverChannel(CHANNEL));
	}

	private DataTestSubscriber<String> createTestSubscriber() {
		return DataTestSubscriber.createWithTimeoutSecs(TIMEOUT_SECS);
	}

}
