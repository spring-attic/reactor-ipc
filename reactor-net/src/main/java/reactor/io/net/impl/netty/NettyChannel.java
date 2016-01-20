/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.impl.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.subscription.EmptySubscription;
import reactor.core.util.ReactiveState;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.impl.netty.tcp.NettyChannelHandlerBridge;

/**
 * {@link ReactiveChannel} implementation that delegates to Netty.
 * @author Stephane Maldini
 * @since 2.5
 */
public class NettyChannel
		extends Flux<Buffer>
		implements ReactiveChannel<Buffer, Buffer>,
		           ReactiveState.Named,
		           ReactiveState.FeedbackLoop, ReactiveState.ActiveUpstream {

	private final Channel ioChannel;
	private final long    prefetch;

	public NettyChannel(long prefetch, Channel ioChannel) {
		this.prefetch = prefetch;
		this.ioChannel = ioChannel;
	}

	@Override
	public Mono<Void> writeWith(final Publisher<? extends Buffer> dataStream) {
		return new PostWritePublisher(dataStream);
	}

	@Override
	public Mono<Void> writeBufferWith(Publisher<? extends Buffer> dataStream) {
		return writeWith(dataStream);
	}

	@Override
	public Flux<Buffer> input() {
		return this;
	}

	@Override
	public Object delegateInput() {
		NettyChannelHandlerBridge bridge = ioChannel.pipeline().get(NettyChannelHandlerBridge.class);
		if(bridge != null){
			return bridge.downstream();
		}
		return null;
	}

	@Override
	public Object delegateOutput() {
		Channel parent = ioChannel.parent();
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
			         .fireUserEventTriggered(new NettyChannelHandlerBridge.ChannelInputSubscriber(subscriber, prefetch));
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
	public ConsumerSpec on() {
		return new NettyConsumerSpec();
	}

	@Override
	public Channel delegate() {
		return ioChannel;
	}

	@Override
	public String toString() {
		return this.getClass()
		           .getName() + " {" +
				"channel=" + ioChannel +
				'}';
	}

	private class NettyConsumerSpec implements ConsumerSpec {

		@Override
		public ConsumerSpec close(final Runnable onClose) {
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
		public ConsumerSpec readIdle(long idleTimeout, final Runnable onReadIdle) {
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
		public ConsumerSpec writeIdle(long idleTimeout,
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

	public static class FuturePublisher<C extends Future> extends Mono<Void> {

		protected final C future;

		public FuturePublisher(C future) {
			this(future, false);
		}

		@SuppressWarnings("unchecked")
		public FuturePublisher(C future, boolean preinit) {
			this.future = future;
			if(preinit) {
				if(future.isSuccess()){
					init(future);
					return;
				}
				future.addListener(new FutureListener<Object>() {
					@Override
					public void operationComplete(Future<Object> future) throws Exception {
						if (future.isSuccess()) {
							init((C) future);
						}
					}
				});
			}
		}

		protected void init(C future) {
		}

		@Override
		@SuppressWarnings("unchecked")
		public final void subscribe(final Subscriber<? super Void> s) {
			future.addListener(new SubscriberFutureBridge(s));
		}

		protected void doComplete(C future, Subscriber<? super Void> s){
			s.onComplete();
		}

		protected void doError(Subscriber<? super Void> s, Throwable throwable){
			s.onError(throwable);
		}

		private final class SubscriberFutureBridge implements GenericFutureListener<Future<?>> {

			private final Subscriber<? super Void> s;

			public SubscriberFutureBridge(Subscriber<? super Void> s) {
				this.s = s;
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {

					}

					@Override
					@SuppressWarnings("unchecked")
					public void cancel() {
						future.removeListener(SubscriberFutureBridge.this);
					}
				});
			}

			@Override
			@SuppressWarnings("unchecked")
			public void operationComplete(Future<?> future) throws Exception {
				if(!future.isSuccess()){
					doError(s, future.cause());
				}
				else {
					doComplete((C)future, s);
				}
			}
		}
	}

	private class PostWritePublisher extends Mono<Void> implements Upstream, FeedbackLoop {

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
		public Object delegateInput() {
			return NettyChannel.this;
		}

		@Override
		public Object delegateOutput() {
			return NettyChannel.this;
		}
	}
}
