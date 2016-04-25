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

package reactor.io.netty.http;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.cookie.Cookie;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Receiver;
import reactor.core.state.Completable;
import reactor.core.subscriber.BaseSubscriber;

import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.tcp.TcpChannel;
import reactor.io.netty.common.NettyChannelHandler;

/**
 * Conversion between Netty types  and Reactor types ({@link NettyHttpChannel}.
 *
 * @author Stephane Maldini
 */
class NettyHttpServerHandler extends NettyChannelHandler {

	final TcpChannel tcpStream;
	     NettyHttpChannel request;

	public NettyHttpServerHandler(
			ChannelHandler<ByteBuf, ByteBuf, NettyChannel> handler,
			TcpChannel tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();
		ctx.read();
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (request == null && io.netty.handler.codec.http.HttpRequest.class.isAssignableFrom(messageClass)) {
			request = new AutoHeaderNettyHttpChannel(msg);


			if (request.isWebsocket()) {
				HttpObjectAggregator agg = new HttpObjectAggregator(65536);
				ctx.pipeline().addBefore(NettyHttpServerHandler.class.getSimpleName(),
						HttpObjectAggregator.class.getSimpleName(),
						agg);
			}

			final Publisher<Void> closePublisher = handler.apply(request);
			final Subscriber<Void> closeSub = new CloseSubscriber(ctx);

			closePublisher.subscribe(closeSub);

		}
		if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, msg);
		}
		postRead(ctx, msg);
	}

	protected void postRead(ChannelHandlerContext ctx, Object msg){
		if (channelSubscriber != null && LastHttpContent.class.isAssignableFrom(msg.getClass())) {
			channelSubscriber.onComplete();
			channelSubscriber = null;
		}
	}
	protected void writeLast(ChannelHandlerContext ctx){
		if(request.markHeadersAsFlushed()){
			ctx.writeAndFlush(request.getNettyResponse());
		}
		ctx
		  .writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
		  .addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	protected ChannelFuture doOnWrite(final Object data, final ChannelHandlerContext ctx) {
		return ctx.write(data);
	}

	final NettyWebSocketServerHandler withWebsocketSupport(String url, String
			protocols, boolean textPlain){
		//prevent further header to be sent for handshaking
		if(!request.markHeadersAsFlushed()){
			log.error("Cannot enable websocket if headers have already been sent");
			return null;
		}
		return new NettyWebSocketServerHandler(url, protocols, this, textPlain);
	}

	@Override
	public String getName() {
		return request != null ? request.getName() : "HTTP Client Connection";
	}

	private class AutoHeaderNettyHttpChannel extends NettyHttpChannel {

		private final Cookies cookies;

		public AutoHeaderNettyHttpChannel(Object msg) {
			super(NettyHttpServerHandler.this.tcpStream, (io.netty.handler.codec.http.HttpRequest) msg);
			this.cookies = Cookies.newServerRequestHolder(headers());
		}

		@Override
		protected void doSubscribeHeaders(Subscriber<? super Void> s) {
			tcpStream.emitWriter(just(getNettyResponse()), s);
		}

		@Override
		public Map<CharSequence, Set<Cookie>> cookies() {
			return cookies.getCachedCookies();
		}
	}

	private class CloseSubscriber implements BaseSubscriber<Void>, Receiver, Completable {

		private final ChannelHandlerContext ctx;
		Subscription subscription;

		public CloseSubscriber(ChannelHandlerContext ctx) {
			this.ctx = ctx;
		}

		@Override
		public void onSubscribe(Subscription s) {
			subscription = s;
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onError(Throwable t) {
			if(t != null && t instanceof IOException && t.getMessage() != null && t.getMessage().contains("Broken " +
					"pipe")){
				if (log.isDebugEnabled()) {
					log.debug("Connection closed remotely", t);
				}
				return;
			}
			log.error("Error processing connection. Closing the channel.", t);
			if (request.markHeadersAsFlushed()) {
				request.delegate()
				       .writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR))
						.addListener(ChannelFutureListener.CLOSE);
			}
		}

		@Override
		public boolean isStarted() {
			return ctx.channel().isActive();
		}

		@Override
		public boolean isTerminated() {
			return !ctx.channel().isOpen();
		}

		@Override
		public Object upstream() {
			return subscription;
		}

		@Override
		public void onComplete() {
			if (ctx.channel().isOpen()) {
				if (log.isDebugEnabled()) {
					log.debug("Close Http Response ");
				}
				writeLast(ctx);
				//ctx.channel().close();
				/*
				ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT).addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if(ctx.channel().isOpen()){
							ctx.channel().close();
						}
					}
				});
				 */
			}
		}
	}
}
