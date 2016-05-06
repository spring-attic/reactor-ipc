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
import java.net.URI;
import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import org.reactivestreams.Publisher;
import reactor.core.flow.Loopback;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Timer;
import reactor.core.state.Completable;
import reactor.core.util.Logger;
import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.tcp.TcpChannel;
import reactor.io.netty.tcp.TcpClient;

/**
 * The base class for a Netty-based Http client.
 *
 * @author Stephane Maldini
 */
public class HttpClient implements Loopback, Completable {


	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create() {
		return create(ClientOptions.create()
		                           .timer(Timer.globalOrNull()).sslSupport());
	}

	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create(ClientOptions options) {
		return new HttpClient(options);
	}

	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create(String address, int port) {
		return create(ClientOptions.create()
		                           .timer(Timer.globalOrNull())
		                           .connect(address, port));
	}

	final TcpBridgeClient client;

	protected HttpClient(final ClientOptions options) {
		this.client = new TcpBridgeClient(options);
	}

	@Override
	public Object connectedInput() {
		return client;
	}

	@Override
	public Object connectedOutput() {
		return client;
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> delete(String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpInbound> delete(String url) {
		return request(HttpMethod.DELETE, url, null);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> get(String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpInbound> get(String url) {
		return request(HttpMethod.GET, url, null);
	}

	/**
	 * HTTP PATCH the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> patch(String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PATCH, url, handler);
	}

	/**
	 * HTTP PATCH the passed URL.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpInbound> patch(String url) {
		return request(HttpMethod.PATCH, url, null);
	}

	@Override
	public boolean isStarted() {
		return client.isStarted();
	}

	@Override
	public boolean isTerminated() {
		return client.isTerminated();
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> post(String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> put(String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PUT, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public Mono<HttpInbound> request(HttpMethod method,
			String url,
			Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		final URI currentURI;
		try {
			if (method == null && url == null) {
				throw new IllegalArgumentException("Method && url cannot be both null");
			}
			currentURI = new URI(parseURL(client.getConnectAddress(), url, false));
		}
		catch (Exception e) {
			return Mono.error(e);
		}

		return new MonoClientRequest(this, currentURI, method, handler);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> ws(String url) {
		return request(HttpMethod.GET,
				parseURL(client.getConnectAddress(), url, true),
				HttpOutbound::upgradeToWebsocket);
	}

	/**
	 * @return
	 */
	public final Mono<Void> shutdown() {
		return client.shutdown();
	}

	Mono<Void> doStart(URI url, ChannelHandler<ByteBuf, ByteBuf, HttpChannel> handler) {

		boolean secure = url.getScheme() != null && (url.getScheme()
		                                                .toLowerCase()
		                                                .equals(HTTPS_SCHEME) || url.getScheme()
		                                                                            .toLowerCase()
		                                                                            .equals(WSS_SCHEME));
		return client.doStart(inoutChannel -> handler.apply(((NettyHttpChannel) inoutChannel)),
				new InetSocketAddress(url.getHost(),
						url.getPort() != -1 ? url.getPort() : (secure ? 443 : 80)),
				secure);
	}

	static String parseURL(InetSocketAddress base, String url, boolean ws) {
		if (!url.startsWith(HTTP_SCHEME) && !url.startsWith(WS_SCHEME)) {
			final String parsedUrl = (ws ? WS_SCHEME : HTTP_SCHEME) + "://";
			if (url.startsWith("/")) {
				return parsedUrl + (base != null ?
						base.getHostName() + ":" + base.getPort() :
						"localhost") + url;
			}
			else {
				return parsedUrl + url;
			}
		}
		else {
			return url;
		}
	}
	final static String WS_SCHEME    = "ws";
	final static String WSS_SCHEME   = "wss";
	final static String HTTP_SCHEME  = "http";
	final static String HTTPS_SCHEME = "https";

	final class TcpBridgeClient extends TcpClient {

		TcpBridgeClient(ClientOptions options) {
			super(options);
		}
		@Override
		protected void bindChannel(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
				SocketChannel ch) {
			TcpChannel netChannel = new TcpChannel(client.getDefaultPrefetchSize(), ch);

			ch.pipeline()
			  .addLast(new HttpClientCodec())
			  .addLast(new NettyHttpClientHandler(handler, netChannel));
		}

		@Override
		protected Mono<Void> doStart(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
				InetSocketAddress address,
				boolean secure) {
			return super.doStart(handler, address, secure);
		}

		@Override
		protected Class<?> logClass() {
			return HttpClient.class;
		}
	}
}
