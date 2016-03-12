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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import reactor.core.flow.Loopback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.timer.Timer;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.config.ServerSocketOptions;
import reactor.io.netty.config.SslOptions;
import reactor.io.netty.http.HttpChannel;
import reactor.io.netty.http.HttpServer;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.tcp.NettyTcpServer;

/**
 * A Netty-based {@code HttpServer} implementation
 * @author Stephane Maldini
 * @since 2.5
 */
public class NettyHttpServer extends HttpServer<Buffer, Buffer> implements Loopback {

	private static final Logger log = Logger.getLogger(NettyHttpServer.class);

	protected NettyTcpServer server;

	protected NettyHttpServer(final Timer timer, final InetSocketAddress listenAddress,
			final ServerSocketOptions options, final SslOptions sslOptions) {

		super(timer);

		this.server = new TcpBridgeServer(timer, listenAddress, options, sslOptions);
	}

	@Override
	public InetSocketAddress getListenAddress() {
		return this.server.getListenAddress();
	}

	@Override
	protected Mono<Void> doStart(
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> defaultHandler) {
		return server.start(ch -> {
			NettyHttpChannel request = (NettyHttpChannel) ch;

			try {
				Publisher<Void> afterHandlers = routeChannel(request);

				if (afterHandlers == null) {
					if (defaultHandler != null) {
						return defaultHandler.apply(request);
					}
					else if (request.markHeadersAsFlushed()) {
						//404
						request.delegate()
						       .writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
					}
					return Flux.empty();

				}
				else {
					return afterHandlers;
				}
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				return Mono.error(t);
			}
			//500
		});
	}

	/**
	 *
	 * @param c
	 * @return
	 */
	public static Mono<Void> upgradeToWebsocket(HttpChannel<?, ?> c){
		return upgradeToWebsocket(c, null);
	}

	/**
	 *
	 * @param c
	 * @param protocols
	 * @return
	 */
	public static Mono<Void> upgradeToWebsocket(HttpChannel<?, ?> c, String protocols){
		ChannelPipeline pipeline = ((SocketChannel) c.delegate()).pipeline();
		NettyHttpWSServerHandler handler = pipeline.remove(NettyHttpServerHandler.class)
		                                           .withWebsocketSupport(c.uri(), protocols);

		if(handler != null) {
			pipeline.addLast(handler);
			return new NettyChannel.FuturePublisher<>(handler.handshakerResult);
		}
		return Mono.error(new IllegalStateException("Failed to upgrade to websocket"));
	}

	@Override
	protected final void onWebsocket(HttpChannel<?, ?> next, String protocols) {
		ChannelPipeline pipeline = ((SocketChannel) next.delegate()).pipeline();
		pipeline.addLast(pipeline.remove(NettyHttpServerHandler.class)
		                         .withWebsocketSupport(next.uri(), protocols));
	}

	@Override
	public boolean isStarted() {
		return server.isStarted();
	}

	@Override
	public boolean isTerminated() {
		return server.isTerminated();
	}

	@Override
	public Object connectedInput() {
		return server;
	}

	@Override
	public Object connectedOutput() {
		return server;
	}

	@Override
	protected final Mono<Void> doShutdown() {
		return server.shutdown();
	}

	protected void bindChannel(
			ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler,
			SocketChannel nativeChannel) {

		NettyChannel netChannel =
				new NettyChannel(getDefaultPrefetchSize(), nativeChannel);

		ChannelPipeline pipeline = nativeChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyHttpServer.class));
		}

		pipeline.addLast(new HttpServerCodec());

		pipeline.addLast(NettyHttpServerHandler.class.getSimpleName(), new NettyHttpServerHandler(handler, netChannel));

	}

	private class TcpBridgeServer extends NettyTcpServer {

		public TcpBridgeServer(Timer timer,
				InetSocketAddress listenAddress,
				ServerSocketOptions options,
				SslOptions sslOptions) {
			super(timer, listenAddress, options, sslOptions);
		}

		@Override
		protected void bindChannel(
				ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler,
				SocketChannel nativeChannel) {
			NettyHttpServer.this.bindChannel(handler, nativeChannel);
		}
	}
}