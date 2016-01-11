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
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

import reactor.Processors;
import reactor.core.processor.FluxProcessor;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.Logger;
import reactor.io.net.ReactiveNet;
import reactor.io.net.nexus.Nexus;
import reactor.rx.Stream;

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

		Random r = new Random();

		// =========================================================

		FluxProcessor<Integer, Integer> p = Processors.emitter();
		Stream<Integer> dispatched = Stream.from(p).dispatchOn(Processors.asyncGroup());

		//slow subscribers
		for(int i = 0; i < 2; i++) {
			dispatched
					.log("slow",  Level.FINEST, Logger.ALL)
					.consume(d ->
						LockSupport.parkNanos(10_000_000 * (r.nextInt(80) + 1))
					);
		}

		//fast subscriber
		dispatched.log("fast",  Level.FINEST, Logger.ALL).consume();


		ReactiveSession<Integer> s1 = p.startSession();
		nexus.monitor(s1);

		// =========================================================

		p = Processors.emitter();
		dispatched = Stream.from(p).dispatchOn(Processors.asyncGroup());

		//slow subscribers
		for(int j = 0; j < 3; j++) {
			dispatched
					.log("slow",  Level.FINEST, Logger.ALL)
					//.capacity(5)
					.consume(d ->
							LockSupport.parkNanos(100_000_000 * (r.nextInt(20) + 1))
					);
		}

		//fast subscriber
		dispatched.log("fast",  Level.FINEST, Logger.ALL).consume();


		ReactiveSession<Integer> s2 = p.startSession();

		nexus.monitor(s2);

		// =========================================================

		p = Processors.emitter();
		dispatched = Stream.from(p).dispatchOn(Processors.asyncGroup());

		//slow subscribers
		for(int j = 0; j < 3; j++) {
			dispatched
					.log("slow",  Level.FINEST, Logger.ALL)
					//.capacity(5)
					.consume(d ->
							LockSupport.parkNanos(1000_000_000)
					);
		}


		ReactiveSession<Integer> s3 = p.startSession();
		nexus.monitor(s3);


		int j = 0;
		for(;;){
			s1.emit(j);
			s2.emit(j);
			s3.emit(j);
			j++;
			LockSupport.parkNanos(30_000_000);
//			if(i == 200){
//				s.failWith(new Exception("LMAO"));
//				break;
//			}
		}

	}
}
