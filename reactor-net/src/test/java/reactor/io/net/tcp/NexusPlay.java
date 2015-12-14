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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import reactor.Processors;
import reactor.Subscribers;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.processor.BaseProcessor;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveNet;
import reactor.io.net.http.HttpClient;
import reactor.io.net.nexus.Nexus;
import reactor.rx.Streams;

/**
 * @author Stephane Maldini
 */
public class NexusPlay {

	public static void main(String... args) throws Exception{


		Nexus nexus = ReactiveNet.nexus();
		nexus.withSystemStats()
		     .startAndAwait();

		//ReactiveStateUtils.scan(o).toString()

		//SAMPLE streams to monitor
		//nexus.monitor(nexus);

		BaseProcessor<Integer, Integer> p = Processors.emitter();
		Random r = new Random();
		Streams.wrap(p)
		        .dispatchOn(Processors.asyncGroup())
		        //.log()
				.multiConsume(4, d -> {
					try{
						Thread.sleep(r.nextInt(80) + 1);
					}
					catch (Exception e){

					}
				});


		ReactiveSession<Integer> s = p.startSession();
		nexus.monitor(s);
		int i = 0;
		for(;;){
			s.emit(i);
			LockSupport.parkNanos(30_000_000);
		}

	}
}
