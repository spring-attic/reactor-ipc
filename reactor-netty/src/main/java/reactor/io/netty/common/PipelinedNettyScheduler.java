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

package reactor.io.netty.common;

import java.util.concurrent.Future;

import io.netty.channel.EventLoop;
import reactor.core.flow.Cancellation;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Stephane Maldini
 */
public final class PipelinedNettyScheduler implements Scheduler {

	final EventLoop eventLoop;
	final Scheduler delegate;

	public PipelinedNettyScheduler(EventLoop eventLoop) {
		this.eventLoop = eventLoop;
		this.delegate = Schedulers.fromExecutor(eventLoop, false);
	}

	@Override
	public Cancellation schedule(Runnable task) {
		if (!eventLoop.inEventLoop()) {
			Future<?> f = eventLoop.submit(task);
			return () -> f.cancel(true);
		}
		else {
			task.run();
			return NOOP;
		}
	}

	@Override
	public void shutdown() {
		eventLoop.shutdownGracefully();
	}

	@Override
	public Worker createWorker() {
		return new NettyColocatedWorker(eventLoop, delegate.createWorker());
	}

	public EventLoop getEventLoop() {
		return eventLoop;
	}

	final static class NettyColocatedWorker implements Worker {

		final Worker    delegate;
		final EventLoop loop;

		public NettyColocatedWorker(EventLoop loop, Worker delegate) {
			this.delegate = delegate;
			this.loop = loop;
		}

		@Override
		public Cancellation schedule(Runnable task) {
			if (!loop.inEventLoop()) {
				return delegate.schedule(task);
			}
			else {
				task.run();
				return NOOP;
			}
		}

		@Override
		public void shutdown() {
			delegate.shutdown();
		}
	}

	static final Cancellation NOOP = () -> {
	};
}
