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
import java.util.function.Consumer;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Loopback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.Subscribers;
import reactor.core.scheduler.Timer;
import reactor.core.tuple.Tuple2;
import reactor.core.util.Assert;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.Reconnect;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.config.SslOptions;
import reactor.io.netty.http.model.Method;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.tcp.NettyTcpClient;

/**
 * A Netty-based {@code HttpClient}.
 * @author Stephane Maldini
 * @since 2.5
 */
public class NettyHttpClient extends HttpClient<Buffer, Buffer> implements Loopback {

	private final static Logger log = Logger.getLogger(NettyHttpClient.class);

	private final NettyTcpClient client;

	private URI lastURI = null;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration
	 * and the given {@code reactor} to send events. The number of IO threads used by the
	 * client is configured by the environment's {@code reactor.tcp.ioThreadCount}
	 * property. In its absence the number of IO threads will be equal to the {@link
	 * reactor.core.publisher.SchedulerGroup#DEFAULT_POOL_SIZE number of available processors}. </p> The
	 * client will connect to the given {@code connectAddress}, configuring its socket
	 * using the given {@code opts}. The given {@code codec} will be used for encoding and
	 * decoding of data.
	 * @param timer The default timer configured
	 * @param connectAddress The root host and port to connect relatively from in http
	 * handlers
	 * @param options The configuration options for the client's socket
	 * @param sslOptions The SSL configuration options for the client's socket
	 */
	public NettyHttpClient(final Timer timer, final InetSocketAddress connectAddress,
			final ClientOptions options, final SslOptions sslOptions) {
		super(timer, options);

		this.client = new TcpBridgeClient(timer, connectAddress, options, sslOptions);
	}

	@Override
	protected Mono<Void> doStart(
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler) {
		return client.start(inoutChannelFlux -> {
			final NettyHttpChannel ch =
					((NettyHttpChannel) inoutChannelFlux);
			return handler.apply(ch);
		});
	}

	@Override
	protected Flux<Tuple2<InetSocketAddress, Integer>> doStart(
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler,
			final Reconnect reconnect) {
		return client.start(inoutChannelFlux -> {
			final NettyHttpChannel ch =
					((NettyHttpChannel) inoutChannelFlux);
			return handler.apply(ch);
		}, reconnect);
	}

	@Override
	public Mono<? extends HttpChannel<Buffer, Buffer>> request(final Method method,
			final String url,
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler) {
		final URI currentURI;
		try {
			Assert.isTrue(method != null && url != null);
			currentURI = parseURL(method, url);
			lastURI = currentURI;
		}
		catch (Exception e) {
			return Mono.error(e);
		}

		return new PostRequestPublisher(currentURI, method, handler);
	}

	private URI parseURL(Method method, String url) throws Exception {
		if (!url.startsWith(HttpChannel.HTTP_SCHEME) && !url.startsWith(HttpChannel.WS_SCHEME)) {
			final String parsedUrl = (method.equals(Method.WS) ? HttpChannel.WS_SCHEME :
					HttpChannel.HTTP_SCHEME) + "://";
			if (url.startsWith("/")) {
				return new URI(parsedUrl + (lastURI != null && lastURI.getHost() != null ?
						lastURI.getHost() : "localhost") + url);
			}
			else {
				return new URI(parsedUrl + url);
			}
		}
		else {
			return new URI(url);
		}
	}

	@Override
	public Object connectedInput() {
		return client;
	}

	@Override
	public Object connectedOutput() {
		return client;
	}

	@Override
	protected final Mono<Void> doShutdown() {
		return client.shutdown();
	}

	protected void bindChannel(
			ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler,
			Object nativeChannel) {
		SocketChannel ch = (SocketChannel) nativeChannel;

		NettyChannel netChannel =
				new NettyChannel(getDefaultPrefetchSize(), ch);

		ChannelPipeline pipeline = ch.pipeline();
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyHttpClient.class));
		}

		pipeline.addLast(new HttpClientCodec());

		URI currentURI = lastURI;
		if (currentURI.getScheme() != null && currentURI.getScheme()
		                                                .toLowerCase()
		                                                .startsWith(HttpChannel.WS_SCHEME)) {
			pipeline.addLast(new HttpObjectAggregator(8192))
			        .addLast(new NettyHttpWSClientHandler(handler, netChannel, WebSocketClientHandshakerFactory.newHandshaker(lastURI, WebSocketVersion.V13, null, false, new DefaultHttpHeaders())));
		}
		else {
			pipeline.addLast(new NettyHttpClientHandler(handler, netChannel));
		}
	}

	@Override
	protected boolean shouldFailOnStarted() {
		return false;
	}

	private class PostRequestPublisher extends Mono<HttpChannel<Buffer, Buffer>> {

		private final URI                                                             currentURI;
		private final Method                                                          method;
		private final ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler;

		public PostRequestPublisher(URI currentURI,
				Method method,
				ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler) {
			this.currentURI = currentURI;
			this.method = method;
			this.handler = handler;
		}

		@Override
		public void subscribe(final Subscriber<? super HttpChannel<Buffer, Buffer>> subscriber) {
			doStart(c -> {
				try {
					URI uri = currentURI;
					NettyHttpChannel ch = (NettyHttpChannel) c;
					ch.getNettyRequest()
					  .setUri(uri.getPath() + (
							  uri.getQuery() == null ? "" :
									  "?" + uri.getQuery()))
					  .setMethod(new HttpMethod(method.getName()))
					  .headers()
					  .add(HttpHeaders.Names.HOST, uri.getHost())
					  .add(HttpHeaders.Names.ACCEPT, "*/*");

					ch.delegate()
					  .pipeline()
					  .fireUserEventTriggered(new NettyHttpClientHandler.ChannelInputSubscriberEvent(subscriber));

					if (handler != null) {
						return handler.apply(ch);
					}
					else {
						ch.headers()
						  .removeTransferEncodingChunked();
						return ch.writeHeaders();
					}
				}
				catch (Throwable t) {
					return Mono.error(t);
				}
			}).subscribe(Subscribers.unbounded(null,
					(Consumer<Throwable>) reason -> EmptySubscription.error(subscriber, reason)));
		}
	}

	@Override
	public boolean isStarted() {
		return client.isStarted();
	}

	@Override
	public boolean isTerminated() {
		return client.isTerminated();
	}

	private class TcpBridgeClient extends NettyTcpClient {

		private final InetSocketAddress connectAddress;

		public TcpBridgeClient(Timer timer,
				InetSocketAddress connectAddress,
				ClientOptions options,
				SslOptions sslOptions) {
			super(timer, connectAddress, options, sslOptions);
			this.connectAddress = connectAddress;
		}

		@Override
		protected void bindChannel(
				ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler,
				SocketChannel nativeChannel) {

			URI currentURI = lastURI;
			try {
				if (currentURI.getScheme() != null && (currentURI.getScheme()
				                                                 .toLowerCase()
				                                                 .equals(HttpChannel.HTTPS_SCHEME) || currentURI.getScheme()
				                                                                                                .toLowerCase()
				                                                                                                .equals(HttpChannel.WSS_SCHEME))) {
					nativeChannel.config()
					             .setAutoRead(true);
					addSecureHandler(nativeChannel);
				} else {
					nativeChannel.config()
					  .setAutoRead(false);
				}
			}
			catch (Exception e) {
				nativeChannel.pipeline()
				             .fireExceptionCaught(e);
			}

			NettyHttpClient.this.bindChannel(handler, nativeChannel);
		}

		@Override
		public InetSocketAddress getConnectAddress() {
			if (connectAddress != null) {
				return connectAddress;
			}
			try {
				URI url = lastURI;
				String host =
						url != null && url.getHost() != null ? url.getHost() :
								"localhost";
				int port = url != null ? url.getPort() : -1;
				if (port == -1) {
					if (url != null && url.getScheme() != null && (url.getScheme()
					                                                  .toLowerCase()
					                                                  .equals(HttpChannel.HTTPS_SCHEME) || url.getScheme()
					                                                                                          .toLowerCase()
					                                                                                          .equals(HttpChannel.WSS_SCHEME))) {
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
	}
}
