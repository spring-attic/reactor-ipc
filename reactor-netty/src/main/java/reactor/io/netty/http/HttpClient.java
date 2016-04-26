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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import reactor.core.flow.Loopback;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Timer;
import reactor.core.util.Logger;
import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.Peer;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.tcp.TcpChannel;
import reactor.io.netty.tcp.TcpClient;

/**
 * The base class for a Netty-based Http client.
 *
 * @author Stephane Maldini
 */
public class HttpClient extends Peer<ByteBuf, ByteBuf, HttpChannel> implements Loopback {


	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient create() {
		return create(ClientOptions.create()
		                           .timer(Timer.globalOrNull()));
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
	final TcpClient client;
	URI lastURI = null;

	protected HttpClient(final ClientOptions options) {
		super(options.timer(), options.prefetch());

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
	 * @param handler the {@link ChannelHandler} to invoke on open channel
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
			currentURI = new URI(parseURL(url, false));
			lastURI = currentURI;
		}
		catch (Exception e) {
			return Mono.error(e);
		}

		return new MonoPostRequest(this, currentURI, method, handler);
	}

	/**
	 * WebSocket to the passed URL.
	 * @param url the target remote URL
	 * @return a {@link Publisher} of the {@link HttpChannel} ready to consume for
	 * response
	 */
	public final Mono<HttpInbound> ws(String url) {
		return request(HttpMethod.GET,
				parseURL(url, true),
				HttpOutbound::upgradeToWebsocket);
	}

	@Override
	protected boolean shouldFailOnStarted() {
		return false;
	}

	@Override
	protected Mono<Void> doStart(ChannelHandler<ByteBuf, ByteBuf, HttpChannel> handler) {
		return client.start(inoutChannel -> {
			final NettyHttpChannel ch = ((NettyHttpChannel) inoutChannel);
			return handler.apply(ch);
		});
	}

	@Override
	protected final Mono<Void> doShutdown() {
		return client.shutdown();
	}

	final void bindChannel(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
			Object nativeChannel) {
		SocketChannel ch = (SocketChannel) nativeChannel;

		TcpChannel netChannel = new TcpChannel(getDefaultPrefetchSize(), ch);

		ChannelPipeline pipeline = ch.pipeline();
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(HttpClient.class));
		}

		pipeline.addLast(new HttpClientCodec())
		        .addLast(new NettyHttpClientHandler(handler, netChannel));
	}

	final String parseURL(String url, boolean ws) {
		if (!url.startsWith(HTTP_SCHEME) && !url.startsWith(WS_SCHEME)) {
			final String parsedUrl = (ws ? WS_SCHEME : HTTP_SCHEME) + "://";
			if (url.startsWith("/")) {
				return parsedUrl + (lastURI != null && lastURI.getHost() != null ? lastURI.getHost() :
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
	final static Logger log = Logger.getLogger(HttpClient.class);

	final class TcpBridgeClient extends TcpClient {

		final InetSocketAddress connectAddress;

		TcpBridgeClient(ClientOptions options) {
			super(options);
			this.connectAddress = options.remoteAddress();
		}

		@Override
		public InetSocketAddress getConnectAddress() {
			if (connectAddress != null) {
				return connectAddress;
			}
			try {
				URI url = lastURI;
				String host = url != null && url.getHost() != null ? url.getHost() : "localhost";
				int port = url != null ? url.getPort() : -1;
				if (port == -1) {
					if (url != null && url.getScheme() != null && (url.getScheme()
					                                                  .toLowerCase()
					                                                  .equals(HTTPS_SCHEME) || url.getScheme()
					                                                                                          .toLowerCase()
					                                                                                          .equals(WSS_SCHEME))) {
						port = 443;
					}
					else {
						port = 80;
					}
				}
				return new InetSocketAddress(host, port);
			}
			catch (Exception e) {
				throw new IllegalArgumentException(e);
			}
		}

		@Override
		protected void bindChannel(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
				SocketChannel nativeChannel) {

			URI currentURI = lastURI;
			try {
				if (currentURI.getScheme() != null && (currentURI.getScheme()
				                                                 .toLowerCase()
				                                                 .equals(HTTPS_SCHEME) || currentURI.getScheme()
				                                                                                                .toLowerCase()
				                                                                                                .equals(WSS_SCHEME))) {
					nativeChannel.config()
					             .setAutoRead(true);
					addSecureHandler(nativeChannel);
				}
				else {
					nativeChannel.config()
					             .setAutoRead(false);
				}
			}
			catch (Exception e) {
				nativeChannel.pipeline()
				             .fireExceptionCaught(e);
			}

			HttpClient.this.bindChannel(handler, nativeChannel);
		}
	}
}
