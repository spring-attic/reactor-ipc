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

import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.cookie.Cookie;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelHandler;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.common.NettyChannelHandler;
import reactor.io.netty.tcp.TcpChannel;

/**
 * @author Stephane Maldini
 */
class NettyHttpClientHandler extends NettyChannelHandler {

	final TcpChannel tcpStream;
	NettyHttpChannel                httpChannel;
	Subscriber<? super HttpInbound> replySubscriber;

	/**
	 * The body of an HTTP response should be discarded.
	 */
	private boolean discardBody = false;

	public NettyHttpClientHandler(ChannelHandler<Buffer, Buffer, NettyChannel> handler, TcpChannel tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();

		if(httpChannel == null) {
			httpChannel = new PostHeaderPublisher();
			httpChannel.keepAlive(true);
			HttpUtil.setTransferEncodingChunked(httpChannel.nettyRequest, true);
		}


		handler.apply(httpChannel)
		       .subscribe(new BaseSubscriber<Void>() {
			       @Override
			       public void onSubscribe(final Subscription s) {
				       ctx.read();
				       BackpressureUtils.validate(null, s);
				       s.request(Long.MAX_VALUE);
			       }

			       @Override
			       public void onError(Throwable t) {
				       BaseSubscriber.super.onError(t);
				       if(t instanceof IOException && t.getMessage() != null && t.getMessage().contains("Broken pipe")){
					       if (log.isDebugEnabled()) {
						       log.debug("Connection closed remotely", t);
					       }
					       return;
				       }
				       if (ctx.channel()
				              .isOpen()) {
					       ctx.channel()
					          .close();
				       }
			       }

			       @Override
			       public void onComplete() {
				       writeLast(ctx);
			       }
		       });
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (HttpResponse.class.isAssignableFrom(messageClass)) {
			HttpResponse response = (HttpResponse) msg;
			if (httpChannel != null) {
				httpChannel.setNettyResponse(response);
			}

			checkResponseCode(ctx, response);

			ctx.fireChannelRead(msg);
			if (FullHttpResponse.class.isAssignableFrom(messageClass)) {
				postRead(ctx, msg);
			}
			if(!discardBody && replySubscriber != null){
				Flux.just(httpChannel).subscribe(replySubscriber);
			}
		} else if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());
			postRead(ctx, msg);
		} else if(!discardBody){
			super.channelRead(ctx, msg);
		}
	}

	private void checkResponseCode(ChannelHandlerContext ctx, HttpResponse response) throws Exception {
		boolean discardBody = false;

		int code = response.status()
		                   .code();
		if (code == HttpResponseStatus.NOT_FOUND.code()
				|| code == HttpResponseStatus.BAD_REQUEST.code()
				|| code == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()) {
			Exception ex = new HttpException(httpChannel);
			exceptionCaught(ctx, ex);
			if(replySubscriber != null){
				EmptySubscription.error(replySubscriber, ex);
			}
			discardBody = true;
		}

		setDiscardBody(discardBody);
	}

	protected void postRead(ChannelHandlerContext ctx, Object msg){
		if (LastHttpContent.class.isAssignableFrom(msg.getClass())) {
			ctx.channel().close();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {

		if (evt != null && evt.getClass().equals(ChannelInputSubscriberEvent.class)) {
			replySubscriber = ((ChannelInputSubscriberEvent)evt).clientReplySubscriber;
		}
		else {
			super.userEventTriggered(ctx, evt);
		}

	}

	@Override
	public String getName() {
		return httpChannel != null ? httpChannel.getName() : "HTTP Client Connection";
	}

	protected void writeLast(final ChannelHandlerContext ctx){
		ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
	}

	private void setDiscardBody(boolean discardBody) {
		this.discardBody = discardBody;
	}

	/**
	 * An event to attach a {@link Subscriber} to the {@link TcpChannel}
	 * created by {@link HttpClient}
	 */
	public static final class ChannelInputSubscriberEvent {

		private final Subscriber<? super HttpInbound> clientReplySubscriber;

		public ChannelInputSubscriberEvent(Subscriber<? super HttpInbound> inputSubscriber) {
			if (null == inputSubscriber) {
				throw new IllegalArgumentException("HTTP receive subscriber must not be null.");
			}
			this.clientReplySubscriber = inputSubscriber;
		}
	}

	class PostHeaderPublisher extends NettyHttpChannel {

		private Cookies cookies;

		public PostHeaderPublisher() {
			super(NettyHttpClientHandler.this.tcpStream, null);
		}

		@Override
		void setNettyResponse(HttpResponse nettyResponse) {
			super.setNettyResponse(nettyResponse);
			this.cookies = Cookies.newClientResponseHolder(responseHeaders());
		}

		@Override
		protected void doSubscribeHeaders(Subscriber<? super Void> s) {
			tcpStream.emitWriter(just(getNettyRequest()), s);
		}

		@Override
		public Map<CharSequence, Set<Cookie>> cookies() {
			return cookies.getCachedCookies();
		}
	}
}
