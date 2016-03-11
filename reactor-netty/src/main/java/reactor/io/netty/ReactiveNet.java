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

package reactor.io.netty;

import java.util.function.Function;

import reactor.core.timer.Timer;
import reactor.core.util.Assert;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.netty.http.HttpClient;
import reactor.io.netty.http.HttpServer;
import reactor.io.netty.http.NettyHttpClient;
import reactor.io.netty.http.NettyHttpServer;
import reactor.io.netty.nexus.Nexus;
import reactor.io.netty.tcp.NettyTcpClient;
import reactor.io.netty.tcp.NettyTcpServer;
import reactor.io.netty.tcp.TcpClient;
import reactor.io.netty.tcp.TcpServer;
import reactor.io.netty.udp.DatagramServer;

/**
 * Reactive Client/Server Network facilities <p>
 * <pre>
 * {@code
 * //echo server
 * ReactiveNet.tcpServer(1234).start( connection -> ch.writeWith(connection) );
 *
 * ReactiveNet.tcpClient(1234).start( connection ->
 *    connection
 *      //Listen for any incoming data on that connection, they will be Buffer an IOStream can easily decode
 *      .nest()
 *      .flatMap(self -> new StringCodec('\n').decode(self))
 *      .consume(log::info);
 *
 *    //Push anything from the publisher returned, here a simple Reactor Flux. By default a Buffer is expected
 *    //Will close after write
 *    return connection.writeWith(Flux.just(Buffer.wrap("hello\n")));
 * });
 *
 * //We can also preconfigure global codecs and other custom client/server parameter with the Function signature:
 * ReactiveNet.tcpServer(spec -> spec.preprocessor(CodecPreprocessor.from(kryoCodec)).listen(1235)).start( intput -> {
 *      input.subscribe(Subscribers.unbounded(log::info));
 *      return input.writeWith(Flux.interval(1_000l));
 * });
 *
 * //Assigning the same codec to a client and a server greatly improve readability and provide for extended type
 * safety.
 * ReactiveNet.tcpServer(spec -> spec.preprocessor(CodecPreprocessor.from(kryoCodec)).connect("localhost", 1235)).start(
 * input -> {
 *      input.subscribe(Subscribers.unbounded(log::info));
 *   return input.writeWith(Flux.just("hello"));
 * });
 *
 * }
 * </pre>
 * @author Stephane Maldini
 */
public enum ReactiveNet {
	;

	public static final int    DEFAULT_PORT         = System.getenv("PORT") != null ?
			Integer.parseInt(System.getenv("PORT")) : 12012;
	public static final String DEFAULT_BIND_ADDRESS = "127.0.0.1";


	/**
	 * Bind a new TCP server to "loopback" on port {@literal 12012}. By default the default server implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> To reply data on the active connection, {@link
	 * ChannelFlux#writeWith} can subscribe to any passed {@link org.reactivestreams.Publisher}. <p> Note that
	 * {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be
	 * flushed every capacity batch size and read will pause when capacity number of elements have been dispatched. <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer() {
		return tcpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP server to "loopback" on the given port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty first is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(int port) {
		return tcpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server
	 * implementation is scanned from the classpath on Class init. Support for Netty first is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of
	 * {@link org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from -
	 * onComplete when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(String bindAddress) {
		return tcpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpServer<Buffer, Buffer> tcpServer(final String bindAddress, final int port) {
		return tcpServer(new Function<Spec.TcpServerSpec<Buffer, Buffer>, Spec.TcpServerSpec<Buffer, Buffer>>() {
			@Override
			public Spec.TcpServerSpec<Buffer, Buffer> apply(Spec.TcpServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timer.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP server to the specified bind address and port. By default the default server implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param configuringFunction a function will apply and return a {@link Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static <IN, OUT> TcpServer<IN, OUT> tcpServer(Function<? super Spec.TcpServerSpec<IN, OUT>, ? extends Spec.TcpServerSpec<IN, OUT>> configuringFunction) {
		return configuringFunction.apply(new Spec.TcpServerSpec<IN, OUT>(NettyTcpServer.class))
		                          .get();
	}

	/**
	 * Bind a new TCP client to the localhost on port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient() {
		return tcpClient(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. By default the default client
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of
	 * {@link org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from -
	 * onComplete when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress the address to connect to on port 12012
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(String bindAddress) {
		return tcpClient(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to connect to on "loopback"
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(int port) {
		return tcpClient(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress the address to connect to
	 * @param port the port to connect to
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient<Buffer, Buffer> tcpClient(final String bindAddress, final int port) {
		return tcpClient(new Function<Spec.TcpClientSpec<Buffer, Buffer>, Spec.TcpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.TcpClientSpec<Buffer, Buffer> apply(Spec.TcpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timer.globalOrNull());
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param configuringFunction a function will apply and return a {@link Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static <IN, OUT> TcpClient<IN, OUT> tcpClient(Function<? super Spec.TcpClientSpec<IN, OUT>, ? extends Spec.TcpClientSpec<IN, OUT>> configuringFunction) {
		return configuringFunction.apply(new Spec.TcpClientSpec<IN, OUT>(NettyTcpClient.class))
		                          .get();
	}

	// HTTP

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 * @return a simple HTTP Server
	 */
	public static HttpServer<Buffer, Buffer> httpServer() {
		return httpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and 12012
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(String bindAddress) {
		return httpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Build a simple Netty HTTP server listening on 127.0.0.1 and the passed port
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(int port) {
		return httpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Build a simple Netty HTTP server listening othe passed bind address and port
	 * @param bindAddress address to listen for (e.g. 0.0.0.0 or 127.0.0.1)
	 * @param port the port to listen to
	 * @return a simple HTTP server
	 */
	public static HttpServer<Buffer, Buffer> httpServer(final String bindAddress, final int port) {
		return httpServer(new Function<Spec.HttpServerSpec<Buffer, Buffer>, Spec.HttpServerSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpServerSpec<Buffer, Buffer> apply(Spec.HttpServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timer.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}

	/**
	 * Build a Netty HTTP Server with the passed factory
	 * @param configuringFunction a factory to build server configuration
	 * @param <IN> incoming data type
	 * @param <OUT> outgoing data type
	 * @return a Netty HTTP server with the passed factory
	 */
	public static <IN, OUT> HttpServer<IN, OUT> httpServer(Function<? super Spec.HttpServerSpec<IN, OUT>, ? extends Spec.HttpServerSpec<IN, OUT>> configuringFunction) {
		return configuringFunction.apply(new Spec.HttpServerSpec<IN, OUT>(NettyHttpServer.class))
		                          .get();
	}

	/**
	 * @return a simple HTTP client
	 */
	public static HttpClient<Buffer, Buffer> httpClient() {
		return httpClient(new Function<Spec.HttpClientSpec<Buffer, Buffer>, Spec.HttpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpClientSpec<Buffer, Buffer> apply(Spec.HttpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timer.globalOrNull());
				return clientSpec;
			}
		});
	}

	/**
	 * Bind a new HTTP client to the specified connect address and port. By default the default server implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the relevant library
	 * dependencies are on the classpath. <p> A {@link HttpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param configuringFunction a function will apply and return a {@link Spec} to customize the peer
	 * @param <IN> the given input type received by this peer. Any configured codec decoder must match this type.
	 * @param <OUT> the given output type received by this peer. Any configured codec encoder must match this type.
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static <IN, OUT> HttpClient<IN, OUT> httpClient(Function<? super Spec.HttpClientSpec<IN, OUT>, ? extends Spec.HttpClientSpec<IN, OUT>> configuringFunction) {
		return configuringFunction.apply(new Spec.HttpClientSpec<IN, OUT>(NettyHttpClient.class))
		                          .get();
	}

	// UDP

	/**
	 * Bind a new UDP server to the "loopback" address. By default the default server implementation is scanned from the
	 * classpath on Class init. Support for Netty is provided as long as the relevant library dependencies are on the
	 * classpath. <p> <p> From the emitted {@link ChannelFlux}, one can decide to add in-channel consumers to read
	 * any incoming data. <p> To reply data on the active connection, {@link ChannelFlux#writeWith} can subscribe to
	 * any passed {@link org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity}
	 * will be used to switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value,
	 * write on flush and auto read will apply. Otherwise, data will be flushed every capacity batch size and read will
	 * pause when capacity number of elements have been dispatched. <p> Emitted channels will run on the same thread
	 * they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static DatagramServer udpServer() {
		return udpServer(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new UDP server to the given bind address. By default the default server implementation is scanned from the
	 * classpath on Class init. Support for Netty is provided as long as the relevant library dependencies are on the
	 * classpath. <p> <p> From the emitted {@link ChannelFlux}, one can decide to add in-channel consumers to read
	 * any incoming data. <p> To reply data on the active connection, {@link ChannelFlux#writeWith} can subscribe to
	 * any passed {@link org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity}
	 * will be used to switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value,
	 * write on flush and auto read will apply. Otherwise, data will be flushed every capacity batch size and read will
	 * pause when capacity number of elements have been dispatched. <p> Emitted channels will run on the same thread
	 * they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static DatagramServer udpServer(String bindAddress) {
		return udpServer(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new UDP server to the "loopback" address and specified port. By default the default server implementation
	 * is scanned from the classpath on Class init. Support for Netty is provided as long as the relevant library
	 * dependencies are on the classpath. <p> <p> From the emitted {@link ChannelFlux}, one can decide to add
	 * in-channel consumers to read any incoming data. <p> To reply data on the active connection, {@link
	 * ChannelFlux#writeWith} can subscribe to any passed {@link org.reactivestreams.Publisher}. <p> Note that
	 * {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be
	 * flushed every capacity batch size and read will pause when capacity number of elements have been dispatched. <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on the passed bind address
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static DatagramServer udpServer(int port) {
		return udpServer(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new UDP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant library dependencies are
	 * on the classpath. <p> <p> From the emitted {@link ChannelFlux}, one can decide to add in-channel consumers to
	 * read any incoming data. <p> To reply data on the active connection, {@link ChannelFlux#writeWith} can
	 * subscribe to any passed {@link org.reactivestreams.Publisher}. <p> Note that {@link
	 * reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read / flush on write
	 * mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be flushed
	 * every capacity batch size and read will pause when capacity number of elements have been dispatched. <p> Emitted
	 * channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static DatagramServer udpServer(final String bindAddress, final int port) {
		return udpServer(serverSpec -> {
			serverSpec.timer(Timer.globalOrNull());
			return serverSpec.listen(bindAddress, port);
		});
	}

	/**
	 * Bind a new UDP server to the specified bind address and port. By default the default server implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the relevant library
	 * dependencies are on the classpath. <p> <p> From the emitted {@link ChannelFlux}, one can decide to add
	 * in-channel consumers to read any incoming data. <p> To reply data on the active connection, {@link
	 * ChannelFlux#writeWith} can subscribe to any passed {@link org.reactivestreams.Publisher}. <p> Note that
	 * {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read / flush on
	 * write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data will be
	 * flushed every capacity batch size and read will pause when capacity number of elements have been dispatched. <p>
	 * Emitted channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param configuringFunction a function will apply and return a {@link Spec} to customize the peer
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static DatagramServer udpServer(Function<? super Spec.DatagramServerSpec, ? extends Spec.DatagramServerSpec> configuringFunction) {
		return configuringFunction.apply(new Spec.DatagramServerSpec(DatagramServer.class))
		                          .get();
	}

	//MONITORING FEED SERVER

	/**
	 * Bind a new Console HTTP server to "loopback" on port {@literal 12012}. By default the default server
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> To reply data on the active connection,
	 * {@link ChannelFlux#writeWith} can subscribe to any passed {@link org.reactivestreams.Publisher}. <p> Note
	 * that {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read /
	 * flush on write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data
	 * will be flushed every capacity batch size and read will pause when capacity number of elements have been
	 * dispatched. <p> Emitted channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus nexus() {
		return nexus(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP server to "loopback" on the given port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus nexus(int port) {
		return nexus(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of
	 * {@link org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from -
	 * onComplete when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus nexus(String bindAddress) {
		return nexus(bindAddress, DEFAULT_PORT);
	}


	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus nexus(final String bindAddress, final int port) {
		return nexus(new Function<Spec.HttpServerSpec<Buffer, Buffer>, Spec.HttpServerSpec<Buffer, Buffer>>() {
			@Override
			public Spec.HttpServerSpec<Buffer, Buffer> apply(Spec.HttpServerSpec<Buffer, Buffer> serverSpec) {
				serverSpec.timer(Timer.globalOrNull());
				return serverSpec.listen(bindAddress, port);
			}
		});
	}
	// TCP

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param configuringFunction a function will apply and return a {@link Spec} to customize the peer
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus nexus(
			Function<? super Spec.HttpServerSpec<Buffer, Buffer>, ? extends Spec.HttpServerSpec<Buffer, Buffer>>
					configuringFunction) {
		return Nexus.create(configuringFunction.apply(new Spec.HttpServerSpec<Buffer, Buffer>(NettyHttpServer.class)).get());
	}

	/**
	 * Utils to read the ChannelFlux underlying channel
	 */

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ChannelFlux<IN, OUT> channel) {
		return (E) delegate(channel, Object.class);
	}

	@SuppressWarnings("unchecked")
	public static <E, IN, OUT> E delegate(ChannelFlux<IN, OUT> channel, Class<E> clazz) {
		Assert.isTrue(clazz.isAssignableFrom(channel.delegate()
		                                                  .getClass()),
				"Underlying channel is not of the given type: " + clazz.getName());

		return (E) channel.delegate();
	}

	/**
	 * @return a Specification to configure and supply a Reconnect handler
	 */
	static public Spec.IncrementalBackoffReconnect backoffReconnect() {
		return new Spec.IncrementalBackoffReconnect();
	}

}
