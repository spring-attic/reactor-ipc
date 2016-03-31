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

package reactor.io.netty.tcp;

import java.net.InetSocketAddress;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.state.Introspectable;
import reactor.core.timer.Timer;
import reactor.core.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.netty.Preprocessor;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.Client;
import reactor.io.netty.ReactiveNet;
import reactor.io.netty.ReactivePeer;
import reactor.io.netty.Reconnect;
import reactor.io.netty.Spec;
import reactor.io.netty.config.ClientSocketOptions;
import reactor.io.netty.config.SslOptions;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpClient<IN, OUT>
		extends Client<IN, OUT, ChannelFlux<IN, OUT>>
		implements Introspectable {

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
	public static TcpClient<Buffer, Buffer> create() {
		return create(ReactiveNet.DEFAULT_BIND_ADDRESS);
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
	public static TcpClient<Buffer, Buffer> create(String bindAddress) {
		return create(bindAddress, ReactiveNet.DEFAULT_PORT);
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
	public static TcpClient<Buffer, Buffer> create(int port) {
		return create(ReactiveNet.DEFAULT_BIND_ADDRESS, port);
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
	public static TcpClient<Buffer, Buffer> create(final String bindAddress, final int port) {
		return ReactiveNet.tcpClient(new Function<Spec.TcpClientSpec<Buffer, Buffer>, Spec.TcpClientSpec<Buffer, Buffer>>() {
			@Override
			public Spec.TcpClientSpec<Buffer, Buffer> apply(Spec.TcpClientSpec<Buffer, Buffer> clientSpec) {
				clientSpec.timer(Timer.globalOrNull());
				return clientSpec.connect(bindAddress, port);
			}
		});
	}

	private final InetSocketAddress   connectAddress;
	private final ClientSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpClient(Timer timer,
	                    InetSocketAddress connectAddress,
	                    ClientSocketOptions options,
	                    SslOptions sslOptions) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.connectAddress = (null != connectAddress ? connectAddress : new InetSocketAddress("127.0.0.1", 3000));
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Get the {@link InetSocketAddress} to which this client must connect.
	 *
	 * @return the connect address
	 */
	public InetSocketAddress getConnectAddress() {
		return connectAddress;
	}

	/**
	 * Get the {@link ClientSocketOptions} currently in effect.
	 *
	 * @return the client options
	 */
	protected ClientSocketOptions getOptions() {
		return this.options;
	}

	/**
	 * Get the {@link SslOptions} current in effect.
	 *
	 * @return the SSL options
	 */
	protected SslOptions getSslOptions() {
		return sslOptions;
	}

	@Override
	public String getName() {
		return "TcpClient:"+getConnectAddress().toString();
	}

	@Override
	public int getMode() {
		return 0;
	}

	@Override
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ChannelFlux<NEWIN, NEWOUT>> doPreprocessor(
			Function<ChannelFlux<IN, OUT>, ? extends ChannelFlux<NEWIN, NEWOUT>> preprocessor) {
		return new PreprocessedTcpClient<>(preprocessor);
	}

	private final class PreprocessedTcpClient<NEWIN, NEWOUT, NEWCONN extends ChannelFlux<NEWIN, NEWOUT>>
			extends TcpClient<NEWIN, NEWOUT> {

		private final Function<ChannelFlux<IN, OUT>, ? extends NEWCONN>
				preprocessor;

		public PreprocessedTcpClient(Function<ChannelFlux<IN, OUT>, ? extends NEWCONN> preprocessor) {
			super(TcpClient.this.getDefaultTimer(), TcpClient.this.getConnectAddress(), TcpClient.this.getOptions(), TcpClient.this.getSslOptions());
			this.preprocessor = preprocessor;
		}

		@Override
		protected Mono<Void> doStart(
				ChannelFluxHandler<NEWIN, NEWOUT, ChannelFlux<NEWIN, NEWOUT>> handler) {
			ChannelFluxHandler<IN, OUT, ChannelFlux<IN, OUT>>
					p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p);
		}

		@Override
		protected Flux<Tuple2<InetSocketAddress, Integer>> doStart(
				ChannelFluxHandler<NEWIN, NEWOUT, ChannelFlux<NEWIN, NEWOUT>> handler,
				Reconnect reconnect) {
			ChannelFluxHandler<IN, OUT, ChannelFlux<IN, OUT>>
					p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p, reconnect);
		}

		@Override
		protected Mono<Void> doShutdown() {
			return TcpClient.this.shutdown();
		}

		@Override
		protected boolean shouldFailOnStarted() {
			return false;
		}
	}
}
