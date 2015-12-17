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
import reactor.Processors;
import reactor.aeron.Context;
import reactor.aeron.support.SignalType;
import reactor.aeron.support.TestAeronInfra;
import reactor.core.processor.BaseProcessor;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.rx.Streams;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class UnicastServiceMessageHandlerTest {

	private UnicastServiceMessageHandler handler;

	private TestAeronInfra aeronInfra;

	private BaseProcessor<Buffer, Buffer> processor;

	@Before
	public void doSetup() {
		processor = Processors.emitter(2);

		aeronInfra = new TestAeronInfra();

		Context context = new Context();

		handler = new UnicastServiceMessageHandler(processor, aeronInfra, context, (Consumer<Void>) aVoid -> {

		}) {
			@Override
			protected SignalSender createSignalSender() {
				return aeronInfra.getSignalSender();
			}

		};
		handler.start();
	}

	@After
	public void doTeardown() {
		handler.shutdown();
	}

	@Test
	public void testItWorks() throws InterruptedException {
		Streams.just(1, 2, 3, 4, 5).map(i -> Buffer.wrap("" + i)).subscribe(processor);

		String receiverChannel1 = "udp://192.168.1.1:12000";
		String sessionId1 = receiverChannel1 + "/1/2";
		handler.handleMore(sessionId1, 1);

		String session1Key = receiverChannel1 + "/1";
		aeronInfra.assertNextSignalPublished(session1Key, 1);

		TestAeronInfra.SignalData lastSignalData = aeronInfra.getSignalDataByKey(session1Key);
		assertThat(lastSignalData.lastSignalType, is(SignalType.Next));
		assertThat(lastSignalData.lastBuffer.asString(), is("1"));

		String receiverChannel2 = "udp://192.168.1.1:12000";
		String sessionId2 = receiverChannel2 + "/11/12";
		handler.handleMore(sessionId2, 3);

		String session2Key = receiverChannel2 + "/11";
		aeronInfra.assertNextSignalPublished(session2Key, 2);

		handler.handleMore(sessionId1, 1);

		aeronInfra.assertNextSignalPublished(session1Key, 1);
		aeronInfra.assertNextSignalPublished(session2Key, 1);
	}

}