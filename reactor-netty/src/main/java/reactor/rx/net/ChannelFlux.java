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

package reactor.rx.net;

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.timer.Timer;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.RemoteFlux;
import reactor.io.ipc.RemoteFluxHandler;

/**
 * An abstract {@link RemoteFlux} implementation that handles the basic interaction and behave as a {@link
 * Flux}.
 *
 * @author Stephane Maldini
 */
public class ChannelFlux<IN, OUT> extends Flux<IN> implements RemoteFlux<IN, OUT> {

	protected static final Logger log = Logger.getLogger(ChannelFlux.class);


	private final RemoteFlux<IN, OUT> actual;
	private final Timer               timer;
	private final long                prefetch;

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ChannelFlux<IN, OUT> wrap(final RemoteFlux<IN, OUT> actual, Timer timer, long prefetch){
		return new ChannelFlux<>(actual, timer, prefetch);
	}

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> RemoteFluxHandler<IN, OUT, RemoteFlux<IN, OUT>> wrap(
			final RemoteFluxHandler<IN, OUT, ChannelFlux<IN, OUT>> actual,
			final Timer timer,
			final long prefetch){

		if(actual == null) return null;

		return stream -> actual.apply(wrap(stream, timer, prefetch));
	}

	protected ChannelFlux(final RemoteFlux<IN, OUT> actual,
							final Timer timer,
	                        long prefetch) {

		this.timer = timer;
		this.actual = actual;
		this.prefetch = prefetch;
	}

	@Override
	public Flux<IN> input() {
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Void> writeWith(final Publisher<? extends OUT> source) {
		final Flux<? extends OUT> sourceStream;

		if (Flux.class.isAssignableFrom(source.getClass())) {
			sourceStream = ((Flux<? extends OUT>) source);
		} else {
			sourceStream = new Flux<OUT>() {
				@Override
				public void subscribe(Subscriber<? super OUT> subscriber) {
					source.subscribe(subscriber);
				}

				@Override
				public long getCapacity() {
					return prefetch;
				}

				@Override
				public Timer getTimer() {
					return timer;
				}
			};
		}

		return new Mono<Void>() {
			@Override
			public void subscribe(Subscriber<? super Void> s) {
				actual.writeWith(sourceStream).subscribe(s);
			}
		};
	}

	@Override
	final public Mono<Void> writeBufferWith(Publisher<? extends Buffer> source) {
		return this.actual.writeBufferWith(source);
	}

	@Override
	public final Timer getTimer() {
		return timer;
	}

	@Override
	final public long getCapacity() {
		return prefetch;
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		actual.input().subscribe(s);
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return actual.remoteAddress();
	}

	@Override
	public ConsumerSpec on() {
		return actual.on();
	}

	@Override
	public Object delegate() {
		return actual.delegate();
	}
}
