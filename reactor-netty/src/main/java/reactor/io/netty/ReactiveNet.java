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
 * TcpServer.create(1234).start( connection -> ch.writeWith(connection) );
 *
 * TcpClient.create(1234).start( connection ->
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
 * HttpServer.create(spec -> spec.preprocessor(CodecPreprocessor.from(kryoCodec)).listen(1235)).start( intput -> {
 *      input.subscribe(Subscribers.unbounded(log::info));
 *      return input.writeWith(Flux.interval(1_000l));
 * });
 *
 * //Assigning the same codec to a client and a server greatly improve readability and provide for extended type
 * safety.
 * HttpClient.create(spec -> spec.preprocessor(CodecPreprocessor.from(kryoCodec)).connect("localhost", 1235)).start(
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
