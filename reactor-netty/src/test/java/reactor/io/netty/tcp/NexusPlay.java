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
package reactor.io.netty.tcp;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.SchedulerGroup;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.util.Logger;
import reactor.io.netty.ReactiveNet;
import reactor.io.netty.nexus.Nexus;

/**
 * @author Stephane Maldini
 */
public class NexusPlay {

	public static void main(String... args) throws Exception{


		Nexus nexus = ReactiveNet.nexus();
		nexus.withSystemStats()
		     .withLogTail()
		     //.useCapacity(5l)
		     .startAndAwait();

		//ReactiveStateUtils.scan(o).toString()

		//SAMPLE streams to monitor
		//create.monitor(create);

		//hotSample(create);
		schedulerGroupSample(nexus);

		CountDownLatch latch = new CountDownLatch(1);
		latch.await();
	}


	static void hotSample(final Nexus nexus){
		new Thread("hotSample"){
			@Override
			public void run() {
				Random r = new Random();

				// =========================================================

				FluxProcessor<Integer, Integer> p = EmitterProcessor.create();
				Flux<Integer> dispatched = p
				                                   .dispatchOn(SchedulerGroup.async("semi-fast",  8192, 4));

				//slow subscribers
				for(int i = 0; i < 2; i++) {
					dispatched
							.log("semi-fast",  Level.FINEST, Logger.ALL)
							.consume(d ->
									LockSupport.parkNanos(100_000 * (r.nextInt(80) + 1))
							);
				}

				//fast subscriber
				dispatched.log("fast",  Level.FINEST, Logger.ALL).subscribe();


				SignalEmitter<Integer> s1 = p.startEmitter();
				nexus.monitor(s1);

				// =========================================================

				p = EmitterProcessor.create();
				dispatched = p.dispatchOn(SchedulerGroup.async("semi-slow", 1024, 4));

				//slow subscribers
				for(int j = 0; j < 3; j++) {
					dispatched
							.log("slow",  Level.FINEST, Logger.ALL)
							//.useCapacity(5)
							.consume(d ->
									LockSupport.parkNanos(10_000_000 * (r.nextInt(20) + 1))
							);
				}

				//fast subscriber
				dispatched.log("fast",  Level.FINEST, Logger.ALL).subscribe();


				SignalEmitter<Integer> s2 = p.startEmitter();

				nexus.monitor(s2);

				// =========================================================

				p = EmitterProcessor.create();
				dispatched = p.dispatchOn(SchedulerGroup.async("slow", 1024, 3));

				//slow subscribers
				for(int j = 0; j < 3; j++) {
					dispatched
							.log("slow",  Level.FINEST, Logger.ALL)
							//.useCapacity(5)
							.consume(d ->
									LockSupport.parkNanos(1000_000_000)
							);
				}


				SignalEmitter<Integer> s3 = p.startEmitter();
				nexus.monitor(s3);


				int j = 0;
				for(;;){
					s1.emit(j);
					s2.emit(j);
					s3.emit(j);
					j++;
					LockSupport.parkNanos(30_000_000);
//			if(j == 200){
//				s2.failWith(new Exception("AHAH"));
//				break;
//			}
				}
			}
		}.start();
	}

	static void schedulerGroupSample(final Nexus nexus){
		new Thread("schedulerGroupSample"){
			@Override
			public void run() {
				SchedulerGroup io = SchedulerGroup.io();
				SchedulerGroup single = SchedulerGroup.single();
				SchedulerGroup async = SchedulerGroup.async();
				nexus.monitor(io);
				nexus.monitor(single);
				nexus.monitor(async);
			}
		}.start();
	}
}
