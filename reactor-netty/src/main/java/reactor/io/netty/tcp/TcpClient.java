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
import java.util.Iterator;
import java.util.function.Supplier;
import javax.net.ssl.SSLEngine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.reactivestreams.Subscriber;
import reactor.core.flow.MultiProducer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Timer;
import reactor.core.state.Introspectable;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.common.MonoChannelFuture;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.NettyChannelHandler;
import reactor.io.netty.common.Peer;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.config.NettyOptions;
import reactor.io.netty.tcp.ssl.SSLEngineSupplier;
import reactor.io.netty.util.NettyNativeDetector;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class TcpClient extends Peer<Buffer, Buffer, NettyChannel> implements Introspectable, MultiProducer {

	public static final ChannelFluxHandler PING = o -> Flux.empty();

	/**
	 * Bind a new TCP client to the localhost on port 12012. By default the default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#send} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient create() {
		return create(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. By default the default client
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of
	 * {@link org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from -
	 * onComplete when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link ChannelFlux#send} can subscribe to any passed {@link
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
	public static TcpClient create(String bindAddress) {
		return create(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#send} can subscribe to any passed {@link
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
	public static TcpClient create(int port) {
		return create(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#send} can subscribe to any passed {@link
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
	public static TcpClient create(String bindAddress, int port) {
		return create(ClientOptions.to(bindAddress, port)
		                           .timer(Timer.globalOrNull()));
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. By default the default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the relevant library
	 * dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#send} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in
	 * auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply.
	 * Otherwise, data will be flushed every capacity batch size and read will pause when capacity number of elements
	 * have been dispatched. <p> Emitted channels will run on the same thread they have beem receiving IO events.
	 * <p>
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 *
	 * @param options
	 *
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static TcpClient create(ClientOptions options) {
		return new TcpClient(options);
	}

	final Bootstrap               bootstrap;
	final EventLoopGroup          ioGroup;
	final Supplier<ChannelFuture> connectionSupplier;
	final ChannelGroup            channelGroup;

	final    ClientOptions     options;
	volatile InetSocketAddress connectAddress;

	protected TcpClient(ClientOptions options) {
		super(options.timer(), options.prefetch());
		this.connectAddress = options.remoteAddress();
		if (null == connectAddress) {
			connectAddress = new InetSocketAddress("127.0.0.1", 3000);
		}

		this.options = options.toImmutable();
		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if (started.get()) {
					return bootstrap.connect(getConnectAddress());
				}
				else {
					return null;
				}
			}
		};
		if (null != options.eventLoopGroup()) {
			this.ioGroup = options.eventLoopGroup();
		}
		else {
			int ioThreadCount = TcpServer.DEFAULT_TCP_THREAD_COUNT;
			this.ioGroup = NettyNativeDetector.newEventLoopGroup(ioThreadCount,
					ExecutorUtils.newNamedFactory("reactor-tcp-io"));
		}

		Bootstrap _bootstrap = new Bootstrap().group(ioGroup)
		                                      .channel(NettyNativeDetector.getChannel(ioGroup))
		                                      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		                                      .option(ChannelOption.AUTO_READ, options.ssl() != null)
				//.remoteAddress(this.connectAddress)
				;

		_bootstrap = _bootstrap.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
		                       .option(ChannelOption.SO_SNDBUF, options.sndbuf())
		                       .option(ChannelOption.SO_KEEPALIVE, options.keepAlive())
		                       .option(ChannelOption.SO_LINGER, options.linger())
		                       .option(ChannelOption.TCP_NODELAY, options.tcpNoDelay())
		                       .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.timeout());

		this.bootstrap = _bootstrap;
		if (options != null && options.isManaged() || NettyOptions.DEFAULT_MANAGED_PEER) {
			log.debug("Client is managed.");
			this.channelGroup = new DefaultChannelGroup(null);
		}
		else {
			log.debug("Client is not managed (Not directly introspectable)");
			this.channelGroup = null;
		}
	}

	@Override
	public long downstreamCount() {
		return channelGroup == null ? -1 : channelGroup.size();
	}

	@Override
	public Iterator<?> downstreams() {
		if (channelGroup == null) {
			return null;
		}
		return new Iterator<Object>() {
			final Iterator<Channel> channelIterator = channelGroup.iterator();

			@Override
			public boolean hasNext() {
				return channelIterator.hasNext();
			}

			@Override
			public Object next() {
				return channelIterator.next()
				                      .pipeline()
				                      .get(NettyChannelHandler.class);
			}
		};
	}

	/**
	 * Get the {@link InetSocketAddress} to which this client must connect.
	 *
	 * @return the connect address
	 */
	public InetSocketAddress getConnectAddress() {
		return connectAddress;
	}

	@Override
	public String getName() {
		return "TcpClient:" + getConnectAddress().toString();
	}

	/**
	 * Get the {@link ClientOptions} currently in effect.
	 *
	 * @return the client options
	 */
	protected ClientOptions getOptions() {
		return this.options;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Mono<Void> doStart(final ChannelFluxHandler<Buffer, Buffer, NettyChannel> handler) {

		final ChannelFluxHandler<Buffer, Buffer, NettyChannel> targetHandler =
				null == handler ? (ChannelFluxHandler<Buffer, Buffer, NettyChannel>) PING : handler;

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				if (channelGroup != null) {
					channelGroup.add(ch);
				}
				bindChannel(targetHandler, ch);
			}
		});

		return new Mono<Void>() {
			@Override
			public void subscribe(Subscriber<? super Void> s) {
				ChannelFuture channelFuture = connectionSupplier.get();

				if (channelFuture == null) {
					throw new IllegalStateException("Connection supplier didn't return any connection");
				}

				new MonoChannelFuture<>(channelFuture).subscribe(s);
			}
		};
	}

	@Override
	protected Mono<Void> doShutdown() {

		if (getOptions() != null && getOptions().eventLoopGroup() != null) {
			return Mono.empty();
		}

		return new MonoChannelFuture<Future<?>>(ioGroup.shutdownGracefully());
	}

	protected void addSecureHandler(SocketChannel ch) throws Exception {
		SSLEngine ssl = new SSLEngineSupplier(options.ssl(), true).get();
		if (log.isDebugEnabled()) {
			log.debug("SSL enabled using keystore {}",
					(null != options.ssl() && null != options.ssl()
					                                         .keystoreFile() ? options.ssl()
					                                                                  .keystoreFile() : "<DEFAULT>"));
		}
		ch.pipeline()
		  .addFirst(new SslHandler(ssl));
	}

	protected void bindChannel(ChannelFluxHandler<Buffer, Buffer, NettyChannel> handler, SocketChannel ch)
			throws Exception {

		if (null != options.ssl()) {
			addSecureHandler(ch);
		}
		else {
			ch.config()
			  .setAutoRead(false);
		}

		TcpChannel netChannel = new TcpChannel(getDefaultPrefetchSize(), ch);

		ChannelPipeline pipeline = ch.pipeline();

		if (null != getOptions() && null != getOptions().pipelineConfigurer()) {
			getOptions().pipelineConfigurer()
			            .accept(pipeline);
		}
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(TcpClient.class));
		}
		pipeline.addLast(new NettyChannelHandler(handler, netChannel));
	}

	@Override
	protected boolean shouldFailOnStarted() {
		return false;
	}

	protected static final Logger log = Logger.getLogger(TcpClient.class);
}
