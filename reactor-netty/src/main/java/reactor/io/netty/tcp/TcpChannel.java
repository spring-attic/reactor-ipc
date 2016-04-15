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
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Loopback;
import reactor.core.flow.Receiver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.state.Completable;
import reactor.core.util.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.Channel;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.NettyChannelHandler;

/**
 * {@link Channel} implementation that delegates to Netty.
 * @author Stephane Maldini
 * @since 2.5
 */
public class TcpChannel
		extends Flux<Buffer>
		implements NettyChannel, Loopback, Completable {

	private final io.netty.channel.Channel ioChannel;
	private final long                     prefetch;

	public TcpChannel(long prefetch, io.netty.channel.Channel ioChannel) {
		this.prefetch = prefetch;
		this.ioChannel = ioChannel;
	}

	@Override
	public Mono<Void> send(final Publisher<? extends Buffer> dataStream) {
		return new PostWritePublisher(dataStream);
	}

	@Override
	public Flux<Buffer> receive() {
		return this;
	}

	@Override
	public Object connectedInput() {
		NettyChannelHandler bridge = ioChannel.pipeline().get(NettyChannelHandler.class);
		if(bridge != null){
			return bridge.downstream();
		}
		return null;
	}

	@Override
	public Object connectedOutput() {
		io.netty.channel.Channel parent = ioChannel.parent();
		SocketAddress remote = ioChannel.remoteAddress();
		SocketAddress local = ioChannel.localAddress();
		String src = local != null ? local.toString() : "";
		String dst = remote != null ? remote.toString() : "";
		if (parent != null) {
		} else {
			String _src = src;
			src = dst;
			dst = _src;
		}

		return src.replaceFirst("localhost", "") +":"+dst.replaceFirst("localhost", "");
	}

	@Override
	public void subscribe(Subscriber<? super Buffer> subscriber) {
		try {
			ioChannel.pipeline()
			         .fireUserEventTriggered(new NettyChannelHandler.ChannelInputSubscriber(subscriber, prefetch));
		}
		catch (Throwable throwable) {
			EmptySubscription.error(subscriber, throwable);
		}
	}

	@Override
	public boolean isStarted() {
		return ioChannel.isActive();
	}

	@Override
	public boolean isTerminated() {
		return !ioChannel.isOpen();
	}

	public void emitWriter(final Publisher<?> encodedWriter,
			final Subscriber<? super Void> postWriter) {

		final ChannelFutureListener postWriteListener = new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				postWriter.onSubscribe(EmptySubscription.INSTANCE);
				if (future.isSuccess()) {
					postWriter.onComplete();
				}
				else {
					postWriter.onError(future.cause());
				}
			}
		};

		if (ioChannel.eventLoop()
		             .inEventLoop()) {

			ioChannel.write(encodedWriter)
			         .addListener(postWriteListener);
		}
		else {
			ioChannel.eventLoop()
			         .execute(new Runnable() {
				         @Override
				         public void run() {
					         ioChannel.write(encodedWriter)
					                  .addListener(postWriteListener);
				         }
			         });
		}
	}

	@Override
	public String getName() {
		return ioChannel.toString();
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return (InetSocketAddress) ioChannel.remoteAddress();
	}

	@Override
	public Lifecycle on() {
		return new NettyLifecycle();
	}

	@Override
	public io.netty.channel.Channel delegate() {
		return ioChannel;
	}

	@Override
	public String toString() {
		return this.getClass()
		           .getName() + " {" +
				"channel=" + ioChannel +
				'}';
	}

	private class NettyLifecycle implements Lifecycle {

		@Override
		public Lifecycle close(final Runnable onClose) {
			ioChannel.pipeline()
			         .addLast(new ChannelDuplexHandler() {
				         @Override
				         public void channelInactive(ChannelHandlerContext ctx)
						         throws Exception {
					         onClose.run();
					         super.channelInactive(ctx);
				         }
			         });
			return this;
		}

		@Override
		public Lifecycle readIdle(long idleTimeout, final Runnable onReadIdle) {
			ioChannel.pipeline()
			         .addFirst(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS) {
				         @Override
				         protected void channelIdle(ChannelHandlerContext ctx,
						         IdleStateEvent evt) throws Exception {
					         if (evt.state() == IdleState.READER_IDLE) {
						         onReadIdle.run();
					         }
					         super.channelIdle(ctx, evt);
				         }
			         });
			return this;
		}

		@Override
		public Lifecycle writeIdle(long idleTimeout,
				final Runnable onWriteIdle) {
			ioChannel.pipeline()
			         .addLast(new IdleStateHandler(0, idleTimeout, 0, TimeUnit.MILLISECONDS) {
				         @Override
				         protected void channelIdle(ChannelHandlerContext ctx,
						         IdleStateEvent evt) throws Exception {
					         if (evt.state() == IdleState.WRITER_IDLE) {
						         onWriteIdle.run();
					         }
					         super.channelIdle(ctx, evt);
				         }
			         });
			return this;
		}
	}

	private class PostWritePublisher extends Mono<Void> implements Receiver, Loopback {

		private final Publisher<? extends Buffer> dataStream;

		public PostWritePublisher(Publisher<? extends Buffer> dataStream) {
			this.dataStream = dataStream;
		}

		@Override
		public void subscribe(Subscriber<? super Void> s) {
			try {
				emitWriter(dataStream, s);
			}
			catch (Throwable throwable) {
				EmptySubscription.error(s, throwable);
			}
		}

		@Override
		public Object upstream() {
			return dataStream;
		}

		@Override
		public Object connectedInput() {
			return TcpChannel.this;
		}

		@Override
		public Object connectedOutput() {
			return TcpChannel.this;
		}
	}
}
