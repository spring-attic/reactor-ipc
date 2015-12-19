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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

import reactor.Processors;
import reactor.Subscribers;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.processor.BaseProcessor;
import reactor.core.publisher.operator.LogOperator;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveNet;
import reactor.io.net.http.HttpClient;
import reactor.io.net.nexus.Nexus;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * @author Stephane Maldini
 */
public class NexusPlay {

	public static void main(String... args) throws Exception{


		Nexus nexus = ReactiveNet.nexus();
		nexus.withSystemStats()
		     .withLogTail()
		     //.capacity(5l)
		     .startAndAwait();

		//ReactiveStateUtils.scan(o).toString()

		//SAMPLE streams to monitor
		//nexus.monitor(nexus);

		AtomicInteger count = new AtomicInteger();

		BaseProcessor<Integer, Integer> p = Processors.emitter();
		Random r = new Random();
		Stream<Integer> dispatched = Streams.wrap(p).dispatchOn(Processors.asyncGroup());

		//slow subscribers
		for(int i = 0; i < 3; i++) {
			dispatched
					.log("slow",  Level.FINEST, LogOperator.ALL)
					//.capacity(5)
					.consume(d ->
						LockSupport.parkNanos(10_000_000 * (r.nextInt(80) + 1))
					);
		}

		//fast subscriber
		dispatched.log("fast",  Level.FINEST, LogOperator.ALL).consume();


		ReactiveSession<Integer> s = p.startSession();
		nexus.monitor(s);
		int i = 0;
		for(;;){
			s.emit(i++);
			LockSupport.parkNanos(30_000_000);
//			if(i == 200){
//				s.failWith(new Exception("LMAO"));
//				break;
//			}
		}

	}
}
