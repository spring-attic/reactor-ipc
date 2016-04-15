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

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import reactor.core.flow.Loopback;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Timer;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.common.MonoChannelFuture;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.Peer;
import reactor.io.netty.config.ServerOptions;
import reactor.io.netty.tcp.TcpChannel;
import reactor.io.netty.tcp.TcpServer;

/**
 * Base functionality needed by all servers that communicate with clients over HTTP.
 * @author Stephane Maldini
 */
public class HttpServer extends Peer<Buffer, Buffer, HttpChannel> implements Loopback {

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 * @return a simple HTTP Server
	 */
	public static HttpServer create() {
		return create(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Build a simple Netty HTTP server listening othe passed bind address and port
	 *
	 * @param options
	 *
	 * @return a simple HTTP server
	 */
	public static HttpServer create(ServerOptions options) {
		return new HttpServer(options);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and the passed port
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer create(int port) {
		return create(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @return a simple HTTP server
	 */
	public static HttpServer create(String bindAddress) {
		return create(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Build a simple Netty HTTP server listening othe passed bind address and port
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer create(String bindAddress, int port) {
		return create(ServerOptions.create()
		                           .timer(Timer.globalOrNull())
		                           .listen(bindAddress, port));
	}

	/**
	 * @param c
	 *
	 * @return
	 */
	public static Mono<Void> upgradeToWebsocket(HttpChannel c) {
		return upgradeToWebsocket(c, null);
	}

	/**
	 * @param c
	 * @param protocols
	 *
	 * @return
	 */
	public static Mono<Void> upgradeToWebsocket(HttpChannel c, String protocols) {
		ChannelPipeline pipeline = c.delegate()
		                            .pipeline();
		NettyWebSocketServerHandler handler = pipeline.remove(NettyHttpServerHandler.class)
		                                              .withWebsocketSupport(c.uri(), protocols);

		if (handler != null) {
			pipeline.addLast(handler);
			return new MonoChannelFuture<>(handler.handshakerResult);
		}
		return Mono.error(new IllegalStateException("Failed to upgrade to websocket"));
	}

	TcpServer       server;
	ChannelMappings channelMappings;

	HttpServer(final ServerOptions options) {
		super(options.timer());
		this.server = new TcpBridgeServer(options);
	}

	@Override
	public Object connectedInput() {
		return server;
	}

	@Override
	public TcpServer connectedOutput() {
		return server;
	}

	/**
	 * Listen for HTTP DELETE on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer delete(String path, final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		route(ChannelMappings.delete(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the File to serve
	 * @return {@code this}
	 */
	public final HttpServer directory(String path, final File directory) {
		directory(path, directory.getAbsolutePath());
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer directory(final String path, final String directory) {
		return directory(path, directory, null);
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer directory(final String path,
			final String directory,
			final Function<HttpChannel, HttpChannel> interceptor) {
		route(ChannelMappings.prefix(path), new ChannelFluxHandler<Buffer, Buffer, HttpChannel>() {
			@Override
			public Publisher<Void> apply(HttpChannel channel) {
				String strippedPrefix = channel.uri()
				                               .replaceFirst(path, "");
				int paramIndex = strippedPrefix.lastIndexOf("?");
				if (paramIndex != -1) {
					strippedPrefix = strippedPrefix.substring(0, paramIndex);
				}

				if (Files.isReadable(Paths.get(directory + strippedPrefix))) {
					Publisher<Buffer> filePub = Buffer.readFile(directory + strippedPrefix);

					if (interceptor != null) {
						return interceptor.apply(channel)
						                  .send(filePub);
					}
					return channel.send(filePub);
				}
				else {
					return Mono.error(Exceptions.CancelException.INSTANCE);
				}
			}
		});
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param file the File to serve
	 * @return {@code this}
	 */
	public final HttpServer file(String path, final File file) {
		file(ChannelMappings.get(path), file.getAbsolutePath(), null);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param filepath the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer file(String path, final String filepath) {
		file(ChannelMappings.get(path), filepath, null);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param filepath the Path to the file to serve
	 * @param interceptor a channel pre-intercepting handler e.g. for content type header
	 * @return {@code this}
	 */
	public final HttpServer file(Predicate<HttpChannel> path,
			final String filepath,
			final Function<HttpChannel, HttpChannel> interceptor) {
		final Publisher<Buffer> file = Buffer.readFile(filepath);
		route(path, channel -> {
			if (interceptor != null) {
				return interceptor.apply(channel)
				                  .send(file);
			}
			return channel.send(file);
		});
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer get(String path, final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		route(ChannelMappings.get(path), handler);
		return this;
	}

	public InetSocketAddress getListenAddress() {
		return this.connectedOutput()
		           .getListenAddress();
	}

	@Override
	public boolean isStarted() {
		return server.isStarted();
	}

	@Override
	public boolean isTerminated() {
		return server.isTerminated();
	}

	/**
	 * Listen for HTTP POST on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer post(String path, final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		route(ChannelMappings.post(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP PUT on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer put(String path, final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		route(ChannelMappings.put(path), handler);
		return this;
	}

	/**
	 * Register an handler for the given Selector condition, incoming connections will query the internal registry to
	 * invoke the matching handlers. Implementation may choose to reply 404 if no route matches.
	 * @param condition a {@link Predicate} to match the incoming connection with registered handler
	 * @param serviceFunction an handler to invoke for the given condition
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public HttpServer route(final Predicate<HttpChannel> condition,
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel> serviceFunction) {

		if (this.channelMappings == null) {
			this.channelMappings = ChannelMappings.newMappings();
		}

		this.channelMappings.add(condition, serviceFunction);
		return this;
	}

	/***
	 * Additional regex matching is available when reactor-bus is on the classpath. Start the server without any global
	 * handler, only the specific routed methods (get, post...) will apply.
	 *
	 * @return a Promise fulfilled when server is started
	 */
	public final Mono<Void> start() {
		return start(null);
	}

	/**
	 * @see this#start()
	 */
	public final void startAndAwait() throws TimeoutException {
		start().get();
	}

	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer ws(String path, final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		return ws(path, handler, null);
	}

	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(CharSequence)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @param protocols
	 * @return {@code this}
	 */
	public final HttpServer ws(String path,
			final ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler,
			final String protocols) {
		return route(ChannelMappings.get(path), channel -> {
			String connection = channel.headers()
			                           .get(HttpHeaderNames.CONNECTION);
			if (connection != null && connection.equals(HttpHeaderValues.UPGRADE.toString())) {
				onWebsocket(channel, protocols);
			}
			return handler.apply(channel);
		});
	}

	@Override
	protected Mono<Void> doStart(final ChannelFluxHandler<Buffer, Buffer, HttpChannel> defaultHandler) {
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
						       .writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1,
								       HttpResponseStatus.NOT_FOUND));
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

	protected final void onWebsocket(HttpChannel next, String protocols) {
		ChannelPipeline pipeline = next.delegate()
		                               .pipeline();
		pipeline.addLast(pipeline.remove(NettyHttpServerHandler.class)
		                         .withWebsocketSupport(next.uri(), protocols));
	}

	protected Publisher<Void> routeChannel(final HttpChannel ch) {

		if (channelMappings == null) {
			return null;
		}

		final Iterator<? extends ChannelFluxHandler<Buffer, Buffer, HttpChannel>> selected =
				channelMappings.apply(ch)
				               .iterator();

		if (!selected.hasNext()) {
			return null;
		}

		ChannelFluxHandler<Buffer, Buffer, HttpChannel> channelHandler = selected.next();

		if (!selected.hasNext()) {
			return channelHandler.apply(ch);
		}

		final List<Publisher<Void>> multiplexing = new ArrayList<>(4);

		multiplexing.add(channelHandler.apply(ch));

		do {
			channelHandler = selected.next();
			multiplexing.add(channelHandler.apply(ch));

		}
		while (selected.hasNext());

		return Flux.concat(Flux.fromIterable(multiplexing));
	}

	@Override
	protected final Mono<Void> doShutdown() {
		return server.shutdown();
	}

	protected void bindChannel(ChannelFluxHandler<Buffer, Buffer, NettyChannel> handler, SocketChannel nativeChannel) {

		TcpChannel netChannel = new TcpChannel(getDefaultPrefetchSize(), nativeChannel);

		ChannelPipeline pipeline = nativeChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(HttpServer.class));
		}

		pipeline.addLast(new HttpServerCodec());

		pipeline.addLast(NettyHttpServerHandler.class.getSimpleName(), new NettyHttpServerHandler(handler, netChannel));

	}

	static final Logger log = Logger.getLogger(HttpServer.class);

	final class TcpBridgeServer extends TcpServer {

		TcpBridgeServer(ServerOptions options) {
			super(options);
		}

		@Override
		protected void bindChannel(ChannelFluxHandler<Buffer, Buffer, NettyChannel> handler,
				SocketChannel nativeChannel) {
			HttpServer.this.bindChannel(handler, nativeChannel);
		}
	}
}
