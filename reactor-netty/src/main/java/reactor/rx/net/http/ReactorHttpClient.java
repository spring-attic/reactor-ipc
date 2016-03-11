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

package reactor.rx.net.http;

import java.util.function.Function;

import reactor.core.publisher.Mono;
import reactor.io.ipc.RemoteFluxHandler;
import reactor.io.netty.http.HttpChannel;
import reactor.io.netty.http.HttpClient;
import reactor.io.netty.http.model.Method;
import reactor.rx.net.ReactorPeer;

/**
 * The base class for a Reactor-based Http peer.
 * @param <IN> The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public final class ReactorHttpClient<IN, OUT> extends ReactorPeer<IN, OUT, HttpClient<IN, OUT>> {

	public static <IN, OUT> ReactorHttpClient<IN,OUT> create(HttpClient<IN, OUT> client) {
		return new ReactorHttpClient<>(client);
	}

	protected ReactorHttpClient(HttpClient<IN, OUT> client) {
		super(client);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> get(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> get(String url) {

		return request(Method.GET, url, null);
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> post(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> put(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.PUT, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> delete(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> delete(String url) {
		return request(Method.DELETE, url, null);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> ws(String url) {
		return request(Method.WS, url, null);
	}

	/**
	 * WebSocket to the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build
	 *
	 * precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpChannelFlux<IN, OUT>> ws(String url,
			final ReactorHttpHandler<IN, OUT> handler) {
		return request(Method.WS, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link RemoteFluxHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public Mono<HttpChannelFlux<IN, OUT>> request(Method method,
			String url,
			final ReactorHttpHandler<IN, OUT> handler){

		return peer.request(method, url,
						HttpChannelFlux.wrapHttp(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
				).map(new Function<HttpChannel<IN, OUT>, HttpChannelFlux<IN, OUT>>() {
			@Override
			public HttpChannelFlux<IN, OUT> apply(HttpChannel<IN, OUT> channel) {
				return HttpChannelFlux.wrap(channel, peer.getDefaultTimer(), peer.getDefaultPrefetchSize());
			}
		});
	}

}
