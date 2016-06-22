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

package reactor.io.netty.common;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Cancellation;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxEmitter;
import reactor.core.queue.QueueSupplier;
import reactor.core.scheduler.Schedulers;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Introspectable;
import reactor.core.state.Prefetchable;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.io.ipc.Channel;
import reactor.io.ipc.ChannelHandler;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * Channel}.
 *
 * @author Stephane Maldini
 */
public class NettyChannelHandler<C extends NettyChannel> extends ChannelDuplexHandler
		implements Introspectable, Producer, Publisher<Object> {

	protected static final Logger log = Logger.getLogger(NettyChannelHandler.class);

	protected final ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler;
	protected final ChannelBridge<C>                               bridgeFactory;
	protected final Flux<Object>                                   input;

	final InboundEmitter inboundEmitter;

	public NettyChannelHandler(
			ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
			ChannelBridge<C> bridgeFactory,
			io.netty.channel.Channel ch) {
		this(handler, bridgeFactory, ch, null);
	}

	@SuppressWarnings("unchecked")
	public NettyChannelHandler(ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
			ChannelBridge<C> bridgeFactory,
			io.netty.channel.Channel ch,
			NettyChannelHandler parent) {
		this.handler = handler;
		if (parent == null) {
			this.inboundEmitter = new InboundEmitter(ch);
			//guard requests/cancel/subscribe
			this.input = Flux.from(this)
			                 .subscribeOn(Schedulers.fromExecutor(ch.eventLoop()));
		}
		else {

			this.inboundEmitter = parent.inboundEmitter;
			this.input = parent.input;
		}
		this.bridgeFactory = bridgeFactory;

	}

	@Override
	public FluxEmitter<Object> downstream() {
		return inboundEmitter;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		handler.apply(bridgeFactory.createChannelBridge(ctx.channel(), input))
		       .subscribe(new CloseSubscriber(ctx));
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		try {
			inboundEmitter.complete();
			super.channelInactive(ctx);
		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			inboundEmitter.fail(err);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		doRead(msg);
	}

	@SuppressWarnings("unchecked")
	protected final void doRead(Object msg) {
		if (msg == null) {
			return;
		}
		try {
			if (msg == Unpooled.EMPTY_BUFFER || msg instanceof EmptyByteBuf) {
				return;
			}
			inboundEmitter.next(msg);
		}
		catch (Throwable err) {
			Exceptions.throwIfFatal(err);
			inboundEmitter.fail(err);
		}
	}

	@Override
	public String getName() {
		return "TCP Connection";
	}

	@Override
	public int getMode() {
		return INNER;
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		if (inboundEmitter.requested != 0L) {
			ctx.read();
		}
		else{
			if(log.isDebugEnabled()) {
				log.debug("Pausing read due to lack of request");
			}
		}
		ctx.fireChannelReadComplete();
	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, final ChannelPromise promise) throws Exception {
		if (msg instanceof Publisher) {
			@SuppressWarnings("unchecked") Publisher<?> data = (Publisher<?>) msg;
			final long capacity = msg instanceof Backpressurable ? ((Backpressurable) data).getCapacity() : Long.MAX_VALUE;

			if (capacity == Long.MAX_VALUE || capacity == -1L) {
				data.subscribe(new FlushOnTerminateSubscriber(ctx, promise));
			}
			else {
				data.subscribe(new FlushOnCapacitySubscriber(ctx, promise, capacity));
			}
		}
		else {
			super.write(ctx, msg, promise);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable err) throws Exception {
		Exceptions.throwIfFatal(err);
		inboundEmitter.fail(err);
	}

	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (Unpooled.EMPTY_BUFFER != data) {
			return ctx.channel().write(data);
		}
		return null;
	}

	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise, final
			Throwable exception) {
		if (ctx.channel()
		       .isOpen()) {
			ChannelFutureListener listener = future -> {
				if (exception != null) {
					promise.tryFailure(exception);
				}
				else if (future.isSuccess()) {
					promise.trySuccess();
				}
				else {
					promise.tryFailure(future.cause());
				}
			};

			if (last != null) {
				ctx.flush();
				last.addListener(listener);
			}
			else {
				ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
				   .addListener(listener);
			}
		}
		else {
			if(exception != null) {
				promise.tryFailure(exception);
			}
			else {
				promise.trySuccess();
			}
		}
	}

	final static class CloseSubscriber implements BaseSubscriber<Void> {

		private final ChannelHandlerContext ctx;

		public CloseSubscriber(ChannelHandlerContext ctx) {
			this.ctx = ctx;
		}

		@Override
		public void onSubscribe(Subscription s) {
			BaseSubscriber.super.onSubscribe(s);
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onError(Throwable t) {
			if(t instanceof IOException && t.getMessage().contains("Broken pipe")){
				if (log.isDebugEnabled()) {
					log.debug("Connection closed remotely", t);
				}
				return;
			}

			log.error("Error processing connection. Closing the channel.", t);

			ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
			   .addListener(ChannelFutureListener.CLOSE);
		}

		@Override
		public void onComplete() {
			ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
			   .addListener(ChannelFutureListener.CLOSE);
		}
	}

	final class FlushOnTerminateSubscriber
			implements BaseSubscriber<Object>, ChannelFutureListener, Loopback {

		private final ChannelHandlerContext ctx;
		private final ChannelPromise        promise;
		ChannelFuture lastWrite;
		Subscription  subscription;

		public FlushOnTerminateSubscriber(ChannelHandlerContext ctx, ChannelPromise promise) {
			this.ctx = ctx;
			this.promise = promise;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (log.isDebugEnabled()) {
				log.debug("Cancel connection");
			}
			if(subscription != null) {
				subscription.cancel();
			}
			subscription = null;
		}

		@Override
		public Object connectedInput() {
			return NettyChannelHandler.this;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (BackpressureUtils.validate(subscription, s)) {
				this.subscription = s;

				ctx.channel()
				   .closeFuture()
				   .addListener(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(final Object w) {
			BaseSubscriber.super.onNext(w);
			if (subscription == null) {
				throw Exceptions.failWithCancel();
			}
			try {
				ChannelFuture cf = doOnWrite(w, ctx);
				lastWrite = cf;
				if (cf != null && log.isDebugEnabled()) {
					cf.addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if (!future.isSuccess()) {
								log.error("write error :" + w, future.cause());
								if (ByteBuf.class.isAssignableFrom(w.getClass())) {
									((ByteBuf) w).resetReaderIndex();
								}
							}
						}
					});
				}
			}
			catch (Throwable t) {
				log.error("Write error for "+w, t);
				onError(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			BaseSubscriber.super.onError(t);
			if (subscription == null) {
				throw new IllegalStateException("already flushed", t);
			}
			log.error("Write error", t);
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);
			doOnTerminate(ctx, lastWrite, promise, t);
		}

		@Override
		public void onComplete() {
			if (subscription == null) {
				throw new IllegalStateException("already flushed");
			}
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);
			doOnTerminate(ctx, lastWrite, promise, null);
		}
	}

	final class FlushOnCapacitySubscriber
			implements Runnable, BaseSubscriber<Object>,
			           ChannelFutureListener, Loopback, Backpressurable, Completable,
			           Cancellable, Receiver, Prefetchable {

		private final ChannelHandlerContext ctx;
		private final ChannelPromise        promise;
		private final long                  capacity;

		private Subscription subscription;
		private long written = 0L;

		private final ChannelFutureListener writeListener = new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess() && future.cause() != null) {
					promise.tryFailure(future.cause());
					if(log.isDebugEnabled()) {
						log.debug("Write error", future.cause());
					}
					return;
				}
				if (capacity == 1L || --written == 0L) {
					if (subscription != null) {
						subscription.request(capacity);
					}
				}
			}
		};

		public FlushOnCapacitySubscriber(ChannelHandlerContext ctx, ChannelPromise promise, long capacity) {
			this.ctx = ctx;
			this.promise = promise;
			this.capacity = capacity;
		}

		@Override
		public long getPending() {
			return ctx.channel().isWritable() ? written : capacity;
		}

		@Override
		public boolean isCancelled() {
			return !ctx.channel().isOpen();
		}

		@Override
		public boolean isStarted() {
			return subscription != null;
		}

		@Override
		public boolean isTerminated() {
			return !ctx.channel().isOpen();
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public Object connectedInput() {
			return NettyChannelHandler.this;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			if (BackpressureUtils.validate(subscription, s)) {
				subscription = s;

				ctx.channel()
				   .closeFuture()
				   .addListener(this);

				s.request(capacity);
			}
		}

		@Override
		public void onNext(Object w) {
			BaseSubscriber.super.onNext(w);
			if (subscription == null) {
				throw Exceptions.failWithCancel();
			}
			try {
				ChannelFuture cf = doOnWrite(w, ctx);
				if (cf != null) {
					cf.addListener(writeListener);
				}
				if (capacity == 1L) {
					ctx.flush();
				}
				else {
					ctx.channel()
					   .eventLoop()
					   .execute(this);
				}
			}
			catch (Throwable t) {
				log.error("Write error for "+w, t);
				onError(t);
				throw Exceptions.failWithCancel();
			}
		}

		@Override
		public void onError(Throwable t) {
			BaseSubscriber.super.onError(t);
			if (subscription == null) {
				throw new IllegalStateException("already flushed", t);
			}
			log.error("Write error", t);
			subscription = null;
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);
			doOnTerminate(ctx, null, promise, t);
		}

		@Override
		public void onComplete() {
			if (subscription == null) {
				throw new IllegalStateException("already flushed");
			}
			subscription = null;
			if (log.isDebugEnabled()) {
				log.debug("Flush Connection");
			}
			ctx.channel()
			   .closeFuture()
			   .removeListener(this);

			doOnTerminate(ctx, null, promise, null);
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (log.isDebugEnabled()) {
				log.debug("Cancel connection");
			}
			if(subscription != null) {
				subscription.cancel();
			}
			subscription = null;
		}

		@Override
		public void run() {
			if (++written == capacity) {
				ctx.flush();
			}
		}

		@Override
		public long limit() {
			return 0;
		}

		@Override
		public Object upstream() {
			return subscription;
		}

		@Override
		public long expectedFromUpstream() {
			return capacity == 1 ? (ctx.channel().isWritable() ? 1 : 0 ) : capacity - written;
		}
	}

	public ChannelHandler<ByteBuf, ByteBuf, NettyChannel> getHandler() {
		return handler;
	}

	@Override
	public void subscribe(Subscriber<? super Object> s) {
		if(log.isDebugEnabled()){
			log.debug("Subscribing inbound receiver [pending: " +
					""+inboundEmitter.getPending()+", done: "+inboundEmitter.done+"]");
		}
		if (inboundEmitter.actual == null) {
			if (inboundEmitter.done) {
				if (inboundEmitter.error != null) {
					EmptySubscription.error(s, inboundEmitter.error);
					return;
				}
				else if (inboundEmitter.getPending() == 0) {
					EmptySubscription.complete(s);
					return;
				}
			}

			inboundEmitter.init(s);
			s.onSubscribe(inboundEmitter);
		}
		else {
			EmptySubscription.error(s,
					new IllegalStateException(
							"Only one connection receive subscriber allowed."));
		}
	}

	static final class InboundEmitter
			implements FluxEmitter<Object>, Cancellation, Subscription, Producer {

		final io.netty.channel.Channel ch;

		/**
		 * guarded by event loop
		 */
		Subscriber<? super Object> actual;

		/**
		 * guarded by event loop
		 */
		boolean caughtUp;

		/**
		 * guarded by event loop
		 */
		Queue<Object> queue;

		volatile boolean done;
		Throwable error;

		volatile Cancellation cancel;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<InboundEmitter, Cancellation> CANCEL =
				AtomicReferenceFieldUpdater.newUpdater(InboundEmitter.class,
						Cancellation.class,
						"cancel");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InboundEmitter> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InboundEmitter.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<InboundEmitter> WIP =
				AtomicIntegerFieldUpdater.newUpdater(InboundEmitter.class, "wip");

		static final Cancellation CANCELLED = () -> {
		};

		public InboundEmitter(io.netty.channel.Channel channel) {
			this.ch = channel;
			CANCEL.lazySet(this, this);
		}

		void init(Subscriber<? super Object> s){
			actual = s;
			CANCEL.lazySet(this, this);
			WIP.lazySet(this, 0);
		}

		@Override
		public void next(Object value) {
			if (value == null) {
				fail(new NullPointerException("value is null"));
				return;
			}
			if (done) {
				Exceptions.onNextDropped(value);
				return;
			}
			if (caughtUp && actual != null) {
				try {
					actual.onNext(value);
				}
				finally {
					ch.read();
					ReferenceCountUtil.release(value);
				}

			}
			else {
				Queue<Object> q = queue;
				if (q == null) {
					q = QueueSupplier.unbounded().get();
					queue = q;
				}
				q.offer(value);
				if (drain()) {
					caughtUp = true;
				}
			}
		}

		@Override
		public void fail(Throwable error) {
			if (error == null) {
				error = new NullPointerException("error is null");
			}
			if (isCancelled() || done) {
				Exceptions.onErrorDropped(error);
				return;
			}
			done = true;
			if (caughtUp && actual != null) {
				actual.onError(error);
			}
			else {
				this.error = error;
				done = true;
				drain();
			}
		}

		@Override
		public boolean isCancelled() {
			return cancel == CANCELLED;
		}

		@Override
		public void complete() {
			if (isCancelled() || done) {
				return;
			}
			done = true;
			if (caughtUp && actual != null) {
				actual.onComplete();
			}
			drain();
		}

		boolean drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return false;
			}

			int missed = 1;

			for (; ; ) {
				final Queue<Object> q = queue;
				final Subscriber<? super Object> a = actual;

				if (a == null) {
					return false;
				}

				long r = requested;
				long e = 0L;

				while (e != r) {
					if (isCancelled()) {
						return false;
					}

					boolean d = done;
					Object v = q != null ? q.poll() : null;
					boolean empty = v == null;

					if (d && empty) {
						cancelResource();
						if (q != null) {
							q.clear();
						}
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return false;
					}

					if (empty) {
						break;
					}

					try {
						a.onNext(v);
					}
					finally {
						ReferenceCountUtil.release(v);
						ch.read();
					}

					e++;
				}

				if (e == r) {
					if (isCancelled()) {
						return false;
					}

					if (done && (q == null || q.isEmpty())) {
						cancelResource();
						if (q != null) {
							q.clear();
						}
						Throwable ex = error;
						if (ex != null) {
							a.onError(ex);
						}
						else {
							a.onComplete();
						}
						return false;
					}
				}

				if (e != 0L) {
					if (r != Long.MAX_VALUE) {
						if (REQUESTED.addAndGet(this, -e) > 0L) {
							ch.read();
						}
					}
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					if (r == Long.MAX_VALUE) {
						ch.config()
						  .setAutoRead(true);
						  ch.read();
						return true;
					}
					return false;
				}
			}
		}

		@Override
		public void setCancellation(Cancellation c) {
			if (!CANCEL.compareAndSet(this, null, c)) {
				if (cancel != CANCELLED && c != null) {
					c.dispose();
				}
			}
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);
				drain();
			}
		}

		void cancelResource() {
			Cancellation c = cancel;
			if (c != CANCELLED) {
				c = CANCEL.getAndSet(this, CANCELLED);
				if (c != null && c != CANCELLED) {
					REQUESTED.lazySet(this, 0L);
					c.dispose();
				}
			}
		}

		@Override
		public void cancel() {
			cancelResource();

			if (WIP.getAndIncrement(this) == 0) {
				Queue<Object> q = queue;
				if (q != null) {
					q.clear();
				}
			}
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		@Override
		public long getPending() {
			return queue != null ? queue.size() : 0;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		void dereference() {
			actual = null;
		}

		@Override
		public void dispose() {
			if (ch.eventLoop()
			      .inEventLoop()) {
				dereference();
			}
			else {
				ch.eventLoop()
				  .execute(this::dereference);
			}

			ch.config()
			  .setAutoRead(false);
		}
	}
}
