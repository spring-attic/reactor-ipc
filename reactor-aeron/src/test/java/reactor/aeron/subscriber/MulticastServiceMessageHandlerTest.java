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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.support.Stepper;
import reactor.aeron.support.TestAeronInfra;
import reactor.core.processor.RingBufferProcessor;
import reactor.io.buffer.Buffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class MulticastServiceMessageHandlerTest {

	private ExecutorService executor;

	@Before
	public void doSetup() {
		executor = Executors.newFixedThreadPool(2);
	}

	@After
	public void doTeardown() {
		executor.shutdownNow();
	}

	@Test
	public void test() throws InterruptedException {
		Publisher<Buffer> publisher = new Publisher<Buffer>() {
			@Override
			public void subscribe(Subscriber<? super Buffer> s) {
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						System.out.println("Requested: " + n);
					}

					@Override
					public void cancel() {
					}
				});
			}
		};
		RingBufferProcessor<Buffer> processor = RingBufferProcessor.create();
		publisher.subscribe(processor);

		MulticastServiceMessageHandler requestHandler = new MulticastServiceMessageHandler(
				processor, new TestAeronInfra(), new Context(), v -> {});

		final Stepper first = new Stepper();
		Runnable flow1 = new Runnable() {
			@Override
			public void run() {
				try {

					first.reached(0);

					requestHandler.incrementCursor();

					first.reached(1);

					requestHandler.incrementCursor();

					first.reached(2);

					requestHandler.incrementCursor();

					first.reached(3);

				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		};

		final Stepper second = new Stepper();
		Runnable flow2 = new Runnable() {
			@Override
			public void run() {
				try {

					second.reached(0);

					requestHandler.handleMore("session-1", 1);

					second.reached(1);

					requestHandler.handleMore("session-1", 1);

					second.reached(2);

					requestHandler.handleMore("session-2", 1);

					second.reached(3);

					requestHandler.handleMore("session-1", 1);

					second.reached(4);

				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		};

		executor.execute(flow1);
		executor.execute(flow2);

		first.awaitAndAllow(0);
		second.awaitAndAllow(0);

		first.awaitAndAllow(1);
		second.awaitAndAllow(1);

		first.await(2);

		second.awaitAndAllow(2);
		second.awaitAndAllow(3);

		first.allow(2);

		first.awaitAndAllow(3);
		second.awaitAndAllow(4);

		assertThat(requestHandler.getCursor(), is(2L));
	}

}