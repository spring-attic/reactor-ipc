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
import reactor.aeron.utils.SignalType;
import reactor.aeron.utils.TestAeronInfra;
import reactor.aeron.utils.TestSignalSender;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.ipc.buffer.Buffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class UnicastServiceMessageHandlerTest {

	private UnicastServiceMessageHandler handler;

	private FluxProcessor<Buffer, Buffer> processor;

	private TestSignalSender signalSender;

	@Before
	public void doSetup() {
		processor = EmitterProcessor.create(2);

		signalSender = new TestSignalSender();

		Context context = Context.create();

		handler = new UnicastServiceMessageHandler(processor, new TestAeronInfra(), context, () -> {}) {
			@Override
			protected SignalSender createSignalSender() {
				return signalSender;
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
		Flux.just(1, 2, 3, 4, 5).map(i -> Buffer.wrap("" + i)).subscribe(processor);

		String sessionId1 = "udp://192.168.1.1:12000" + "/1";
		handler.handleMore(sessionId1, 1);

		signalSender.assertNumSignalsPublished(sessionId1, 1);

		TestSignalSender.PublicationData data = signalSender.getPublicationDataBySessionId(sessionId1);
		assertThat(data.lastSignalType, is(SignalType.Next));
		assertThat(data.lastSignal.asString(), is("1"));

		String sessionId2 = "udp://192.168.1.1:12000" + "/11";
		handler.handleMore(sessionId2, 3);

		signalSender.assertNumSignalsPublished(sessionId2, 2);

		handler.handleMore(sessionId1, 1);

		signalSender.assertNumSignalsPublished(sessionId1, 1);
		signalSender.assertNumSignalsPublished(sessionId2, 1);
	}

}