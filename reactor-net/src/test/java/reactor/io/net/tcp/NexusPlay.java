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
package reactor.io.net.tcp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import reactor.Subscribers;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.subscription.ReactiveSession;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveNet;
import reactor.io.net.http.HttpClient;
import reactor.io.net.nexus.Nexus;

/**
 * @author Stephane Maldini
 */
public class NexusPlay {

	public static void main(String... args) throws Exception{

		Nexus nexus2 = ReactiveNet.nexus(12014);
		nexus2.startAndAwait();

		Nexus nexus = ReactiveNet.nexus();
		nexus.withSystemStats()
		     .federate("ws://localhost:12014/nexus/stream")
		     .startAndAwait();


		nexus.monitor(nexus);

		HttpClient<Buffer, Buffer> client = ReactiveNet.httpClient();

		client
		           .ws("ws://localhost:12012/nexus/stream")
				   .subscribe(Subscribers.consumer( ch -> {
						ch.input().subscribe(Subscribers.consumer(b -> {
							System.out.println(b);
						}));
					}));

		nexus2.monitor(nexus2);

		CountDownLatch latch = new CountDownLatch(1);

		latch.await();
	}
}
