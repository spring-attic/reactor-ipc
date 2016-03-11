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

package reactor.io.netty.udp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.Future;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SchedulerGroup;
import reactor.core.timer.Timer;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.NettyChannel;
import reactor.io.netty.ReactivePeer;
import reactor.io.netty.config.ServerSocketOptions;
import reactor.io.netty.tcp.NettyChannelHandlerBridge;
import reactor.io.netty.util.NettyNativeDetector;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class DatagramServer
		extends ReactivePeer<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> {

	public static final int DEFAULT_UDP_THREAD_COUNT = Integer.parseInt(
	  System.getProperty("reactor.udp.ioThreadCount",
		"" + SchedulerGroup.DEFAULT_POOL_SIZE)
	);
	protected final Bootstrap      bootstrap;
	protected final EventLoopGroup ioGroup;

	private final InetSocketAddress   listenAddress;
	private final NetworkInterface    multicastInterface;
	private final ServerSocketOptions options;
	private volatile DatagramChannel channel;

	protected DatagramServer(Timer timer,
	                         InetSocketAddress listenAddress,
	                         NetworkInterface multicastInterface,
	                         ServerSocketOptions options) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.listenAddress = listenAddress;
		this.multicastInterface = multicastInterface;
		this.options = options;

		if (null != options && null != options.eventLoopGroup()) {
			this.ioGroup = options.eventLoopGroup();
		} else {
			int ioThreadCount = DEFAULT_UDP_THREAD_COUNT;
			this.ioGroup =
					options == null || options.protocolFamily() == null ?
							NettyNativeDetector.newEventLoopGroup(ioThreadCount, ExecutorUtils.newNamedFactory
									("reactor-udp-io")) :
							new NioEventLoopGroup(ioThreadCount, ExecutorUtils.newNamedFactory
									("reactor-udp-io"));
		}

		this.bootstrap = new Bootstrap()
				.group(ioGroup)
				.option(ChannelOption.AUTO_READ, false)
		;

		if ((options == null || options.protocolFamily() == null) &&
				NettyNativeDetector.getDatagramChannel(ioGroup).getSimpleName().startsWith("Epoll")) {
			bootstrap.channel(NettyNativeDetector.getDatagramChannel(ioGroup));
		} else {
			bootstrap.channelFactory(() -> new NioDatagramChannel(toNettyFamily(options != null ? options.protocolFamily() : null)));
		}

		if (options != null) {
			bootstrap.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
			         .option(ChannelOption.SO_SNDBUF, options.sndbuf())
			         .option(ChannelOption.SO_REUSEADDR, options.reuseAddr())
			         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.timeout());
		}

		if (null != listenAddress) {
			bootstrap.localAddress(listenAddress);
		} else {
			bootstrap.localAddress(NetUtil.LOCALHOST, 3000);
		}
		if (null != multicastInterface) {
			bootstrap.option(ChannelOption.IP_MULTICAST_IF, multicastInterface);
		}
	}

	protected InternetProtocolFamily toNettyFamily(ProtocolFamily family) {
		if (family == null) {
			return null;
		}
		switch (family.name()) {
			case "INET":
				return InternetProtocolFamily.IPv4;
			case "INET6":
				return InternetProtocolFamily.IPv6;
			default:
				throw new IllegalArgumentException("Unsupported protocolFamily: " + family.name());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Mono<Void> doStart(final ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> channelHandler) {
		return new Mono<Void>(){

			@Override
			public void subscribe(final Subscriber<? super Void> subscriber) {
				ChannelFuture future = bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
					@Override
					public void initChannel(final DatagramChannel ch) throws Exception {
						if (null != getOptions() && null != getOptions().pipelineConfigurer()) {
							getOptions().pipelineConfigurer().accept(ch.pipeline());
						}

						bindChannel(channelHandler, ch);
					}
				}).bind();
				future.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						subscriber.onSubscribe(EmptySubscription.INSTANCE);
						if (future.isSuccess()) {
							log.info("BIND {}", future.channel().localAddress());
							channel = (DatagramChannel) future.channel();
							subscriber.onComplete();
						}
						else {
							Exceptions.throwIfFatal(future.cause());
							subscriber.onError(future.cause());
						}
					}
				});
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Mono<Void> doShutdown() {
		return new NettyChannel.FuturePublisher<ChannelFuture>(channel.close()){
			@Override
			protected void doComplete(ChannelFuture future, Subscriber<? super Void> s) {
				if (null == getOptions() || null == getOptions().eventLoopGroup()) {
					new NettyChannel.FuturePublisher<>(ioGroup.shutdownGracefully()).subscribe(s);
				}
				else {
					super.doComplete(future, s);
				}
			}
		};
	}

	public Mono<Void> join(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = channel.joinGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.joinGroup(multicastAddress);
		}

		return new NettyChannel.FuturePublisher<Future<?>>(future){
			@Override
			protected void doComplete(Future<?> future, Subscriber<? super Void> s) {
				log.info("JOIN {}", multicastAddress);
				super.doComplete(future, s);
			}
		};
	}

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to join
	 * @return a {@link Publisher} that will be complete when the group has been joined
	 */
	public Mono<Void> join(InetAddress multicastAddress) {
		return join(multicastAddress, null);
	}

	public Mono<Void> leave(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = channel.leaveGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.leaveGroup(multicastAddress);
		}

		return new NettyChannel.FuturePublisher<Future<?>>(future){
			@Override
			protected void doComplete(Future<?> future, Subscriber<? super Void> s) {
				log.info("LEAVE {}", multicastAddress);
				super.doComplete(future, s);
			}
		};
	}

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to leave
	 * @return a {@link Publisher} that will be complete when the group has been left
	 */
	public Mono<Void> leave(InetAddress multicastAddress) {
		return leave(multicastAddress, null);
	}

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return the bind address
	 */
	public InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link NetworkInterface} on which multicast will be performed.
	 *
	 * @return the multicast NetworkInterface
	 */
	public NetworkInterface getMulticastInterface() {
		return multicastInterface;
	}

	/**
	 * Get the {@link ServerSocketOptions} currently in effect.
	 *
	 * @return the server options in use
	 */
	protected ServerSocketOptions getOptions() {
		return options;
	}


	protected void bindChannel(ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler,
			Object _ioChannel) {
		DatagramChannel ioChannel = (DatagramChannel) _ioChannel;
		NettyChannel netChannel = new NettyChannel(
				getDefaultPrefetchSize(),
				ioChannel
		);

		ChannelPipeline pipeline = ioChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(DatagramServer.class));
		}

		pipeline.addLast(
				new NettyChannelHandlerBridge(handler, netChannel) {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
						if (msg != null && DatagramPacket.class.isAssignableFrom(msg.getClass())) {
							super.channelRead(ctx, ((DatagramPacket) msg).content());
						} else {
							super.channelRead(ctx, msg);
						}
					}

					@Override
					public String getName() {
						return "UDP Connection";
					}
				},
				new ChannelOutboundHandlerAdapter() {
					@Override
					public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
						super.write(ctx, msg, promise);
					}
				});
	}

	private final static Logger log = Logger.getLogger(DatagramServer.class);

}
