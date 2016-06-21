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

package reactor.io.netty.config;

import java.util.concurrent.Future;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import reactor.core.flow.Cancellation;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Stephane Maldini
 */
public class NettyScheduler implements Scheduler {

	final EventLoopGroup eventLoopGroup;
	final FastThreadLocal<Scheduler> localLoop = new FastThreadLocal<>();

	public NettyScheduler(EventLoopGroup eventLoopGroup) {
		this.eventLoopGroup = eventLoopGroup;
		for (EventExecutor ex : eventLoopGroup) {
			ex.execute(() -> {
				if(!localLoop.isSet()){
					localLoop.set(Schedulers.fromExecutor(ex, true));
				}
			});
		}
	}

	@Override
	public Cancellation schedule(Runnable task) {
		Future<?> f = eventLoopGroup.submit(task);
		return () -> f.cancel(true);
	}

	@Override
	public void shutdown() {
		for (EventExecutor ex : eventLoopGroup) {
			ex.execute(() -> localLoop.set(null));
		}
		eventLoopGroup.shutdownGracefully();
	}

	@Override
	public Worker createWorker() {
		if (localLoop.isSet()) {
			return localLoop.get().createWorker();
		}
		for(EventExecutor ex : eventLoopGroup){
			if(ex.inEventLoop()){
				return Schedulers.fromExecutor(ex, true).createWorker();
			}
		}
		return Schedulers.fromExecutor(eventLoopGroup.next(), true).createWorker();
	}

	public EventLoopGroup getEventLoopGroup() {
		return eventLoopGroup;
	}
}
