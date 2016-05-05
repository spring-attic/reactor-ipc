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
import javax.net.ssl.SSLException;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import reactor.core.flow.MultiProducer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Timer;
import reactor.core.state.Introspectable;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.Logger;
import reactor.io.ipc.Channel;
import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.MonoChannelFuture;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.NettyChannelHandler;
import reactor.io.netty.common.Peer;
import reactor.io.netty.config.ClientOptions;
import reactor.io.netty.config.NettyOptions;
import reactor.io.netty.util.NettyNativeDetector;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class TcpClient extends Peer<ByteBuf, ByteBuf, NettyChannel> implements Introspectable, MultiProducer {

	public static final ChannelHandler PING = o -> Flux.empty();

	/**
	 * Bind a new TCP client to the localhost on port 12012. The default client implementation is scanned
	 * from the classpath on Class init. Support for Netty first is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link Channel} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * Channel}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link Channel#send} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> The type of emitted data or received data is {@link ByteBuf}
	 * @return a new Stream of Channel, typically a peer of connections.
	 */
	public static TcpClient create() {
		return create(DEFAULT_BIND_ADDRESS);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port 12012. The default client
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of
	 * {@link org.reactivestreams.Publisher} that will emit: - onNext {@link Channel} to consume data from -
	 * onComplete when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link Channel}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link Channel#send} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> The type of emitted data or received data is {@link ByteBuf}
	 * @param bindAddress the address to connect to on port 12012
	 * @return a new Stream of Channel, typically a peer of connections.
	 */
	public static TcpClient create(String bindAddress) {
		return create(bindAddress, DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP client to "loopback" on the the specified port. The default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link Channel} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * Channel}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link Channel#send} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> The type of emitted data or received data is {@link ByteBuf}
	 * @param port the port to connect to on "loopback"
	 * @return a new Stream of Channel, typically a peer of connections.
	 */
	public static TcpClient create(int port) {
		return create(DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. The default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the
	 * relevant library dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link Channel} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * Channel}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link Channel#send} can subscribe to any passed {@link
	 * org.reactivestreams.Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> The type of emitted data or received data is {@link ByteBuf}
	 * @param bindAddress the address to connect to
	 * @param port the port to connect to
	 * @return a new Stream of Channel, typically a peer of connections.
	 */
	public static TcpClient create(String bindAddress, int port) {
		return create(ClientOptions.to(bindAddress, port)
		                           .timer(Timer.globalOrNull()));
	}

	/**
	 * Bind a new TCP client to the specified connect address and port. The default client implementation is
	 * scanned from the classpath on Class init. Support for Netty is provided as long as the relevant library
	 * dependencies are on the classpath. <p> A {@link TcpClient} is a specific kind of {@link
	 * org.reactivestreams.Publisher} that will emit: - onNext {@link Channel} to consume data from - onComplete
	 * when client is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * Channel}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link Channel#send} can subscribe to any passed {@link org.reactivestreams.Publisher}.
	 * <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in
	 * auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply.
	 * Otherwise, data will be flushed every capacity batch size and read will pause when capacity number of elements
	 * have been dispatched. <p> Emitted channels will run on the same thread they have beem receiving IO events.
	 * <p>
	 * <p> The type of emitted data or received data is {@link ByteBuf}
	 *
	 * @param options
	 *
	 * @return a new Stream of Channel, typically a peer of connections.
	 */
	public static TcpClient create(ClientOptions options) {
		return new TcpClient(options);
	}

	final Bootstrap               bootstrap;
	final EventLoopGroup          ioGroup;
	final ChannelGroup            channelGroup;
	final ClientOptions           options;
	final SslContext              sslContext;

	final InetSocketAddress connectAddress;

	protected TcpClient(ClientOptions options) {
		super(options.timer(), options.prefetch());
		if (null == options.remoteAddress()) {
			this.connectAddress = new InetSocketAddress("127.0.0.1", 3000);
		}
		else{
			this.connectAddress = options.remoteAddress();
		}

		this.options = options.toImmutable();
		if (null != options.eventLoopGroup()) {
			this.ioGroup = options.eventLoopGroup();
		}
		else {
			int ioThreadCount = TcpServer.DEFAULT_TCP_THREAD_COUNT;
			this.ioGroup = NettyNativeDetector.newEventLoopGroup(ioThreadCount,
					ExecutorUtils.newNamedFactory("reactor-tcp-io"));
		}

		if(options.ssl() != null){
			try{
				sslContext = options.ssl().build();

				if (log.isDebugEnabled()) {
					log.debug("Connecting with SSL enabled using context {}",
							sslContext.getClass().getSimpleName());
				}
			}
			catch (SSLException ssle){
				throw Exceptions.bubble(ssle);
			}
		}
		else{
			sslContext = null;
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
		if (options.isManaged() || NettyOptions.DEFAULT_MANAGED_PEER) {
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
			final Iterator<io.netty.channel.Channel> channelIterator = channelGroup.iterator();

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
	protected Mono<Void> doStart(final ChannelHandler<ByteBuf, ByteBuf, NettyChannel>
			handler){
		return doStart(handler, getConnectAddress(), sslContext != null);
	}

	@SuppressWarnings("unchecked")
	protected Mono<Void> doStart(final ChannelHandler<ByteBuf, ByteBuf, NettyChannel>
			handler, InetSocketAddress address, boolean secure) {

		final ChannelHandler<ByteBuf, ByteBuf, NettyChannel> targetHandler =
				null == handler ? (ChannelHandler<ByteBuf, ByteBuf, NettyChannel>) PING : handler;

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				if (channelGroup != null) {
					channelGroup.add(ch);
				}

				if(secure){
					addSecureHandler(ch);
				}
				bindChannel(targetHandler, ch);
			}
		});

		return Mono.defer(() ->
			MonoChannelFuture.from(bootstrap.connect(address))
		);
	}

	@Override
	protected Mono<Void> doShutdown() {

		if (getOptions() != null && getOptions().eventLoopGroup() != null) {
			return Mono.empty();
		}

		return MonoChannelFuture.from(ioGroup.shutdownGracefully());
	}

	final protected void addSecureHandler(SocketChannel ch){
		if (null != sslContext) {
			ch.pipeline().addFirst(sslContext.newHandler(ch.alloc()));
		}
		else {
			ch.config()
			  .setAutoRead(false);
		}
	}

	protected void bindChannel(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler, SocketChannel ch)
			throws Exception {

		ChannelPipeline pipeline = ch.pipeline();

		TcpChannel netChannel = new TcpChannel(getDefaultPrefetchSize(), ch);


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
