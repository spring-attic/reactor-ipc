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

package reactor.io.netty.http;

import java.net.InetSocketAddress;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.timer.Timer;
import reactor.core.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.Client;
import reactor.io.netty.ReactiveNet;
import reactor.io.netty.Reconnect;
import reactor.io.netty.Spec;
import reactor.io.netty.config.ClientSocketOptions;
import reactor.io.netty.http.model.Method;

/**
 * The base class for a Reactor-based Http client.
 * @param <IN> The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class HttpClient<IN, OUT>
		extends Client<IN, OUT, HttpChannel<IN, OUT>> {

	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient<Buffer, Buffer> create() {
		return ReactiveNet.httpClient(new Function<Spec.HttpClientSpec<Buffer, Buffer>, Spec.HttpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpClientSpec<Buffer, Buffer> apply(Spec.HttpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timer.globalOrNull());
				return clientSpec;
			}
		});
	}

	protected HttpClient(Timer timer, ClientSocketOptions options) {
		super(timer, options != null ? options.prefetch() : 1);
	}


	/**
	 * Start this {@literal Peer}.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * HttpClient} is started
	 */
	public final <NEWIN, NEWOUT> Mono<Void> startWithHttpCodec(
			final ChannelFluxHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler,
			final Function<HttpChannel<IN, OUT>, ? extends HttpChannel<NEWIN, NEWOUT>> preprocessor) {

		if (!started.compareAndSet(false, true) && shouldFailOnStarted()) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(ch -> handler.apply(preprocessor.apply(ch)));
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> get(String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> get(String url) {

		return request(Method.GET, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> post(String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> put(String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.PUT, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> delete(String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> delete(String url) {
		return request(Method.DELETE, url, null);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> ws(String url) {
		return request(Method.WS, url, null);
	}

	/**
	 * WebSocket to the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build
	 *
	 * precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<? extends HttpChannel<IN, OUT>> ws(String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return request(Method.WS, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link ChannelFluxHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public abstract Mono<? extends HttpChannel<IN, OUT>> request(Method method,
			String url,
			final ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>> handler);


	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 * @return
	 */
	public <NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>> HttpClient<NEWIN, NEWOUT> httpProcessor(
			final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor
	){
		return new PreprocessedHttpClient<>(preprocessor);
	}

	private final class PreprocessedHttpClient<NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>>
			extends HttpClient<NEWIN, NEWOUT> {

		private final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN>
				preprocessor;

		public PreprocessedHttpClient(
				HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor) {
			super(HttpClient.this.getDefaultTimer(), null);
			this.preprocessor = preprocessor;
		}

		@Override
		public Mono<? extends HttpChannel<NEWIN, NEWOUT>> request(Method method,
				String url,
				final ChannelFluxHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler) {
			return
					HttpClient.this.request(method, url, handler != null ?
							(ChannelFluxHandler<IN, OUT, HttpChannel<IN, OUT>>) conn -> handler.apply(preprocessor.transform(conn)) : null).map(new Function<HttpChannel<IN, OUT>, HttpChannel<NEWIN, NEWOUT>>() {
				@Override
				public HttpChannel<NEWIN, NEWOUT> apply(HttpChannel<IN, OUT> channel) {
					return preprocessor.transform(channel);
				}
			});
		}

		@Override
		protected Flux<Tuple2<InetSocketAddress, Integer>> doStart(
				final ChannelFluxHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler,
				Reconnect reconnect) {
			return HttpClient.this.start(conn -> handler.apply(preprocessor.transform(conn)), reconnect);
		}

		@Override
		protected Mono<Void> doStart(
				final ChannelFluxHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler) {
			return HttpClient.this.start(conn -> handler.apply(preprocessor.transform(conn)));
		}

		@Override
		protected Mono<Void> doShutdown() {
			return HttpClient.this.shutdown();
		}
	}
}
