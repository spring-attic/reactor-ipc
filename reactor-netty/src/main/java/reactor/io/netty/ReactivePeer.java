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

package reactor.io.netty;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import reactor.core.publisher.Mono;
import reactor.core.state.Completable;
import reactor.core.scheduler.Timer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 * <p> A Peer is network component with start and shutdown capabilities. On Start it will
 * require a {@link ChannelFluxHandler} to process the incoming {@link
 * ChannelFlux}, regardless of being a server or a client.
 * @author Stephane Maldini
 */
public abstract class ReactivePeer<IN, OUT, CONN extends ChannelFlux<IN, OUT>>
		implements Completable {

	private final   Timer         defaultTimer;
	private final   long          defaultPrefetch;
	protected final AtomicBoolean started;

	protected ReactivePeer(Timer defaultTimer) {
		this(defaultTimer, Long.MAX_VALUE);
	}

	protected ReactivePeer(Timer defaultTimer, long prefetch) {
		this.defaultTimer = defaultTimer == null && Timer.hasGlobal() ? Timer.global() :
				defaultTimer;
		this.defaultPrefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.started = new AtomicBoolean();
	}

	/**
	 * Start this {@literal Peer}.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public final Mono<Void> start(
			final ChannelFluxHandler<IN, OUT, CONN> handler) {

		if (!started.compareAndSet(false, true) && shouldFailOnStarted()) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(handler);
	}

	/**
	 * Start this {@literal Peer}.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public final <NEWIN, NEWOUT> Mono<Void> startWithCodec(
			final ChannelFluxHandler<NEWIN, NEWOUT, ChannelFlux<NEWIN, NEWOUT>> handler,
			final Function<ChannelFlux<IN, OUT>, ? extends ChannelFlux<NEWIN, NEWOUT>> preprocessor) {

		if (!started.compareAndSet(false, true) && shouldFailOnStarted()) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(ch -> handler.apply(preprocessor.apply(ch)));
	}

	/**
	 * @see this#start(ChannelFluxHandler)
	 * @param handler
	 * @throws InterruptedException
	 */
	public final void startAndAwait(final ChannelFluxHandler<IN, OUT, CONN> handler)
			throws InterruptedException {
		start(handler).get();
	}

	/**
	 * Shutdown this {@literal Peer} and complete the returned {@link Mono<Void>}
	 * when shut down.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public final Mono<Void> shutdown() {
		if (started.compareAndSet(true, false)) {
			return doShutdown();
		}
		return Mono.empty();
	}

	/**
	 * @see this#shutdown()
	 * @throws InterruptedException
	 */
	public final void shutdownAndAwait()
			throws InterruptedException {
		shutdown().get();
	}

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final <NEWIN, NEWOUT, NEWCONN extends ChannelFlux<NEWIN, NEWOUT>, P extends ReactivePeer<NEWIN, NEWOUT, NEWCONN>> P preprocessor(
			final Function<CONN, NEWCONN> preprocessor){

		checkPreprocessor(preprocessor);

		return (P)doPreprocessor(preprocessor);
	}


	/* Implementation Contract */

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @return
	 */
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ChannelFlux<NEWIN, NEWOUT>> doPreprocessor(
			final Function<CONN, ? extends ChannelFlux<NEWIN, NEWOUT>> preprocessor
	){
		return new PreprocessedReactivePeer<>(preprocessor);
	}

	protected final void checkPreprocessor(
			Function<?, ?> preprocessor) {
		Objects.requireNonNull("Preprocessor argument must be provided");

		if (started.get()) {
			throw new IllegalStateException("Peer already started");
		}
	}

	/**
	 * Get the default environment for all Channel
	 * @return The default environment
	 */
	public final Timer getDefaultTimer() {
		return defaultTimer;
	}

	/**
	 * Get the default batch read/write size
	 * @return the default capacity, default Long.MAX for unbounded
	 */
	public final long getDefaultPrefetchSize() {
		return defaultPrefetch;
	}

	protected abstract Mono<Void> doStart(
			ChannelFluxHandler<IN, OUT, CONN> handler);

	protected abstract Mono<Void> doShutdown();

	protected boolean shouldFailOnStarted() {
		return true;
	}

	@Override
	public boolean isStarted() {
		return started.get();
	}

	@Override
	public boolean isTerminated() {
		return !started.get();
	}

	private final class PreprocessedReactivePeer<NEWIN, NEWOUT, NEWCONN extends ChannelFlux<NEWIN, NEWOUT>>
			extends ReactivePeer<NEWIN, NEWOUT, NEWCONN> {

		private final Function<CONN, ? extends NEWCONN> preprocessor;

		public PreprocessedReactivePeer(Function<CONN, ? extends NEWCONN> preprocessor) {
			super(ReactivePeer.this.defaultTimer, ReactivePeer.this.defaultPrefetch);
			this.preprocessor = preprocessor;
		}

		@Override
		protected Mono<Void> doStart(
				final ChannelFluxHandler<NEWIN, NEWOUT, NEWCONN> handler) {
			ChannelFluxHandler<IN, OUT, CONN> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return ReactivePeer.this.start(p);
		}

		@Override
		protected Mono<Void> doShutdown() {
			return ReactivePeer.this.shutdown();
		}
	}

}
