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

package reactor.io.net.http;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.core.error.CancelException;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.core.timer.Timer;
import reactor.io.IO;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.model.HttpHeaders;
import reactor.io.net.http.routing.ChannelMappings;

/**
 * Base functionality needed by all servers that communicate with clients over HTTP.
 * @param <IN> The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Stephane Maldini
 */
public abstract class HttpServer<IN, OUT> extends ReactivePeer<IN, OUT, HttpChannel<IN, OUT>> {

	protected ChannelMappings<IN, OUT> channelMappings;

	protected HttpServer(Timer timer) {
		super(timer);
	}

	/***
	 * Additional regex matching is available when reactor-bus is on the classpath. Start the server without any global
	 * handler, only the specific routed methods (get, post...) will apply.
	 * @return a Promise fulfilled when server is started
	 */
	public final Publisher<Void> start() {
		return start(null);
	}

	/**
	 * @see this#start()
	 */
	public final void startAndAwait() throws InterruptedException {
		Publishers.toReadQueue(start())
		          .take();
	}

	/**
	 * Get the address to which this server is bound. If port 0 was used on configuration, try resolving the port.
	 * @return the bind address
	 */
	public abstract InetSocketAddress getListenAddress();

	/**
	 * Register an handler for the given Selector condition, incoming connections will query the internal registry to
	 * invoke the matching handlers. Implementation may choose to reply 404 if no route matches.
	 * @param condition a {@link Predicate} to match the incoming connection with registered handler
	 * @param serviceFunction an handler to invoke for the given condition
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public HttpServer<IN, OUT> route(final Predicate<HttpChannel> condition,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> serviceFunction) {

		if (this.channelMappings == null) {
			this.channelMappings = ChannelMappings.newMappings();
		}

		this.channelMappings.add(condition, serviceFunction);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> get(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(ChannelMappings.get(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP POST on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> post(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(ChannelMappings.post(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP PUT on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> put(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(ChannelMappings.put(path), handler);
		return this;
	}

	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> ws(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return ws(path, handler, null);
	}

	/**
	 * Listen for WebSocket on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @param protocols
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> ws(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler,
			final String protocols) {
		return route(ChannelMappings.get(path), new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<IN, OUT> channel) {
				String connection = channel.headers()
				                           .get(HttpHeaders.CONNECTION);
				if (connection != null && connection.equals(HttpHeaders.UPGRADE)) {
					onWebsocket(channel, protocols);
				}
				return handler.apply(channel);
			}
		});
	}

	/**
	 * Listen for HTTP DELETE on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param handler an handler to invoke for the given condition
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> delete(String path,
			final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		route(ChannelMappings.delete(path), handler);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param file the File to serve
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> file(String path, final File file) {
		file(ChannelMappings.get(path), file.getAbsolutePath(), null);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param filepath the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> file(String path, final String filepath) {
		file(ChannelMappings.get(path), filepath, null);
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param filepath the Path to the file to serve
	 * @param interceptor a channel pre-intercepting handler e.g. for content type header
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> file(Predicate<HttpChannel> path, final String filepath,
			final Function<HttpChannel<IN, OUT>, HttpChannel<IN, OUT>> interceptor) {
		final Publisher<Buffer> file = IO.readFile(filepath);
		route(path, new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<IN, OUT> channel) {
				if(interceptor != null){
					return interceptor.apply(channel).writeBufferWith(file);
				}
				return channel.writeBufferWith(file);
			}
		});
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the File to serve
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> directory(String path, final File directory) {
		directory(path, directory.getAbsolutePath());
		return this;
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> directory(final String path, final String directory) {
		return directory(path, directory, null);
	}

	/**
	 * Listen for HTTP GET on the passed path to be used as a routing condition. Incoming connections will query the
	 * internal registry to invoke the matching handlers. <p> Additional regex matching is available when reactor-bus is
	 * on the classpath. e.g. "/test/{param}". Params are resolved using {@link HttpChannel#param(String)}
	 * @param path The {@link ChannelMappings.HttpPredicate} to resolve against this path, pattern matching and capture
	 * are supported
	 * @param directory the Path to the file to serve
	 * @return {@code this}
	 */
	public final HttpServer<IN, OUT> directory(final String path, final String directory,
			final Function<HttpChannel<IN, OUT>, HttpChannel<IN, OUT>> interceptor) {
		route(ChannelMappings.prefix(path), new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<IN, OUT> channel) {
				String strippedPrefix = channel.uri()
				                               .replaceFirst(path, "");
				int paramIndex = strippedPrefix.lastIndexOf("?");
				if(paramIndex != -1){
					strippedPrefix = strippedPrefix.substring(0, paramIndex);
				}

				if(Files.isReadable(Paths.get(directory + strippedPrefix))) {
					Publisher<Buffer> filePub = IO.readFile(directory + strippedPrefix);

					if (interceptor != null) {
						return interceptor.apply(channel)
						                  .writeBufferWith(filePub);
					}
					return channel.writeBufferWith(filePub);
				}
				else{
					return Publishers.error(CancelException.get());
				}
			}
		});
		return this;
	}

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 * @return
	 */
	public <NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>> HttpServer<NEWIN, NEWOUT> httpProcessor(final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor) {
		return new PreprocessedHttpServer<>(preprocessor);
	}

	protected abstract void onWebsocket(HttpChannel<?, ?> next, String protocols);

	protected Publisher<Void> routeChannel(final HttpChannel<IN, OUT> ch) {

		if (channelMappings == null) {
			return null;
		}

		final Iterator<? extends ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> selected =
				channelMappings.apply(ch)
				               .iterator();

		if (!selected.hasNext()) {
			return null;
		}

		ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> channelHandler = selected.next();

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

		return Publishers.concat(Publishers.from(multiplexing));
	}

	private final class PreprocessedHttpServer<NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>>
			extends HttpServer<NEWIN, NEWOUT> {

		private final HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor;

		public PreprocessedHttpServer(HttpProcessor<IN, OUT, ? super HttpChannel<IN, OUT>, NEWIN, NEWOUT, NEWCONN> preprocessor) {
			super(HttpServer.this.getDefaultTimer());
			this.preprocessor = preprocessor;
		}

		@Override
		public HttpServer<NEWIN, NEWOUT> route(Predicate<HttpChannel> condition,
				final ReactiveChannelHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> serviceFunction) {
			HttpServer.this.route(condition, new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
				@Override
				public Publisher<Void> apply(HttpChannel<IN, OUT> conn) {
					return serviceFunction.apply(preprocessor.transform(conn));
				}
			});
			return this;
		}

		@Override
		public InetSocketAddress getListenAddress() {
			return HttpServer.this.getListenAddress();
		}

		@Override
		protected void onWebsocket(HttpChannel<?, ?> next, String protocols) {
			HttpServer.this.onWebsocket(next, protocols);
		}

		@Override
		protected Publisher<Void> doStart(final ReactiveChannelHandler<NEWIN, NEWOUT, HttpChannel<NEWIN, NEWOUT>> handler) {
			return HttpServer.this.start(null != handler ? new ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
				@Override
				public Publisher<Void> apply(HttpChannel<IN, OUT> conn) {
					return handler.apply(preprocessor.transform(conn));
				}
			} : null);
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return HttpServer.this.shutdown();
		}
	}
}
