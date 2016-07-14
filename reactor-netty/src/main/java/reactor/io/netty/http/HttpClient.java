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
import reactor.core.Loopback;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.ChannelBridge;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.tcp.TcpChannel;
import reactor.io.netty.tcp.TcpClient;

/**
 * The base class for a Netty-based Http client.
 *
 * @author Stephane Maldini
 */
public class HttpClient
		implements Loopback {


	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create() {
		return create(ClientOptions.create()
		                           .timer(Schedulers.timer()).sslSupport());
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
	public static HttpClient create(String address) {
		return create(address, 80);
	}

	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create(String address, int port) {
		return create(ClientOptions.create()
		                           .timer(Schedulers.timer())
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

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> delete(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.DELETE, url, handler);
	}

	/**
	 * HTTP DELETE the passed URL. When connection has been made, the passed handler is invoked and can be used to build
	 * precisely the request and write data to it.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpClientResponse> delete(String url) {
		return request(HttpMethod.DELETE, url, null);
	}

	/**
	 * HTTP GET the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> get(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.GET, url, handler);
	}

	/**
	 * HTTP GET the passed URL.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpClientResponse> get(String url) {
		return request(HttpMethod.GET, url, null);
	}

	/**
	 * HTTP PATCH the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> patch(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PATCH, url, handler);
	}

	/**
	 * HTTP PATCH the passed URL.
	 *
	 * @param url the target remote URL
	 *
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for response
	 */
	public final Mono<HttpClientResponse> patch(String url) {
		return request(HttpMethod.PATCH, url, null);
	}

	/**
	 * Is shutdown
	 * @return is shutdown
	 */
	public boolean isShutdown() {
		return client.isShutdown();
	}

	/**
	 * HTTP POST the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> post(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.POST, url, handler);
	}

	/**
	 * HTTP PUT the passed URL. When connection has been made, the passed handler is
	 * invoked and can be used to build precisely the request and write data to it.
	 * @param url the target remote URL
	 * @param handler the {@link ChannelHandler} to invoke on open channel
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> put(String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		return request(HttpMethod.PUT, url, handler);
	}

	/**
	 * Use the passed HTTP method to send to the given URL. When connection has been made,
	 * the passed handler is invoked and can be used to build precisely the request and
	 * write data to it.
	 * @param method the HTTP method to send
	 * @param url the target remote URL
	 * @param handler the {@link Function} to invoke on opened TCP connection
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public Mono<HttpClientResponse> request(HttpMethod method,
			String url,
			Function<? super HttpClientRequest, ? extends Publisher<Void>> handler) {
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
		return new MonoHttpClientChannel(this, currentURI, method, handler);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Mono} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpClientResponse> ws(String url) {
		return request(HttpMethod.GET,
				parseURL(client.getConnectAddress(), url, true),
				HttpClientRequest::upgradeToWebsocket);
	}

	/**
	 * @return
	 */
	public final Mono<Void> shutdown() {
		return client.shutdown();
	}

	Mono<Void> doStart(URI url, ChannelBridge<? extends
			TcpChannel> bridge, ChannelHandler<ByteBuf, ByteBuf, HttpChannel> handler) {

		boolean secure = url.getScheme() != null && (url.getScheme()
		                                                .toLowerCase()
		                                                .equals(HTTPS_SCHEME) || url.getScheme()
		                                                                            .toLowerCase()
		                                                                            .equals(WSS_SCHEME));
		return client.doStart(inoutChannel -> handler.apply(((NettyHttpChannel) inoutChannel)),
				new InetSocketAddress(url.getHost(),
						url.getPort() != -1 ? url.getPort() : (secure ? 443 : 80)), bridge,
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

	@SuppressWarnings("unchecked")
	final class TcpBridgeClient extends TcpClient {

		TcpBridgeClient(ClientOptions options) {
			super(options);
		}

		@Override
		protected void bindChannel(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
				SocketChannel ch,
				ChannelBridge<? extends TcpChannel> channelBridge) {
			ch.pipeline()
			  .addLast(new HttpClientCodec())
			  .addLast(new NettyHttpClientHandler(handler,
					  (ChannelBridge<HttpClientChannel>) channelBridge, ch));
		}

		@Override
		protected Mono<Void> doStart(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
				InetSocketAddress address,
				ChannelBridge<? extends TcpChannel> channelBridge,
				boolean secure) {
			return super.doStart(handler, address, channelBridge, secure);
		}

		@Override
		protected Class<?> logClass() {
			return HttpClient.class;
		}

		@Override
		protected ClientOptions getOptions() {
			return super.getOptions();
		}
	}
}
