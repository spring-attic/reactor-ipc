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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.scheduler.Timer;
import reactor.core.state.Completable;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 * <p> A Peer is network component with start and shutdown capabilities. On Start it will
 * require a {@link ChannelFluxHandler} to process the incoming {@link ChannelFlux},
 * regardless of being a server or a client.
 *
 * @author Stephane Maldini
 */
public abstract class Peer<IN, OUT, CONN extends ChannelFlux<IN, OUT>>
		implements Completable {

	public static final int    DEFAULT_PORT         =
			System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 12012;
	public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";

	protected final AtomicBoolean  started;

	final           TimedScheduler defaultTimer;
	final           long           defaultPrefetch;

	protected Peer(TimedScheduler defaultTimer) {
		this(defaultTimer, Long.MAX_VALUE);
	}

	protected Peer(TimedScheduler defaultTimer, long prefetch) {
		this.defaultTimer = defaultTimer == null && Timer.hasGlobal() ? Timer.global() :
				defaultTimer;
		this.defaultPrefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.started = new AtomicBoolean();
	}

	/**
	 * Get the default batch read/write size
	 * @return the default capacity, default Long.MAX for unbounded
	 */
	public final long getDefaultPrefetchSize() {
		return defaultPrefetch;
	}

	/**
	 * Get the default environment for all Channel
	 * @return The default environment
	 */
	public final TimedScheduler getDefaultTimer() {
		return defaultTimer;
	}

	@Override
	public boolean isStarted() {
		return started.get();
	}

	@Override
	public boolean isTerminated() {
		return !started.get();
	}

	/**
	 * Shutdown this {@literal Peer} and complete the returned {@link Mono<Void>}
	 * when shut down.
	 *
	 * @return a {@link Mono<Void>} that will be complete when the {@link Peer} is shutdown
	 */
	public final Mono<Void> shutdown() {
		if (started.compareAndSet(true, false)) {
			return doShutdown();
		}
		return Mono.empty();
	}

	/* Implementation Contract */

	/**
	 * @see this#shutdown()
	 * @throws InterruptedException
	 */
	public final void shutdownAndAwait()
			throws InterruptedException {
		shutdown().get();
	}

	/**
	 * Start this {@literal Peer}.
	 *
	 * @param handler
	 *
	 * @return a {@link Mono<Void>} that will be complete when the {@link Peer} is started
	 */
	public final Mono<Void> start(
			final ChannelFluxHandler<IN, OUT, CONN> handler) {

		if (!started.compareAndSet(false, true) && shouldFailOnStarted()) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(handler);
	}

	/**
	 * @see this#start(ChannelFluxHandler)
	 *
	 * @param handler
	 *
	 * @throws InterruptedException
	 */
	public final void startAndAwait(final ChannelFluxHandler<IN, OUT, CONN> handler)
			throws InterruptedException {
		start(handler).get();
	}

	protected abstract Mono<Void> doStart(
			ChannelFluxHandler<IN, OUT, CONN> handler);

	protected abstract Mono<Void> doShutdown();

	protected boolean shouldFailOnStarted() {
		return true;
	}

}
