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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.state.Completable;
import reactor.core.util.Assert;
import reactor.core.util.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.netty.common.NettyChannel;
import reactor.io.netty.http.model.Cookie;
import reactor.io.netty.http.model.HttpHeaders;
import reactor.io.netty.http.model.Method;
import reactor.io.netty.http.model.Protocol;
import reactor.io.netty.http.model.ResponseHeaders;
import reactor.io.netty.http.model.Status;
import reactor.io.netty.http.model.Transfer;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
abstract class NettyHttpChannel extends Flux<Buffer>
		implements HttpChannel<Buffer, Buffer>, Loopback, Completable {

	private final NettyChannel             tcpStream;
	private final HttpRequest              nettyRequest;
	private final NettyHttpHeaders         headers;
	private       HttpResponse             nettyResponse;
	private       NettyHttpResponseHeaders responseHeaders;
	private volatile int statusAndHeadersSent = 0;
	private Function<? super String, Map<String, Object>> paramsResolver;
	public NettyHttpChannel(NettyChannel tcpStream,
	                        HttpRequest request
	) {
		this.tcpStream = tcpStream;
		this.nettyRequest = request;
		this.nettyResponse = new DefaultHttpResponse(request.getProtocolVersion(), HttpResponseStatus.OK);
		this.headers = new NettyHttpHeaders(request);
		this.responseHeaders = new NettyHttpResponseHeaders(this.nettyResponse);
		this.
		responseHeader(ResponseHeaders.TRANSFER_ENCODING, "chunked");
	}

	@Override
	public HttpChannel<Buffer, Buffer> addCookie(String name, Cookie cookie) {
		if (statusAndHeadersSent == 0) {
			doAddCookie(name, cookie);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	/**
	 * Accumulate a Request Header using the given name and value, appending ";" for each
	 * new value
	 * @return this
	 */
	@Override
	public HttpChannel<Buffer, Buffer> addHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public HttpChannel<Buffer, Buffer> addResponseCookie(String name, Cookie cookie) {
		if (statusAndHeadersSent == 0) {
			doAddResponseCookie(name, cookie);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	/**
	 * Accumulate a response HTTP header for the given key name, appending ";" for each
	 * new value
	 * @param name the HTTP response header name
	 * @param value the HTTP response header value
	 * @return this
	 */
	@Override
	public HttpChannel<Buffer, Buffer> addResponseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddResponseHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public final Object connectedInput() {
		return tcpStream;
	}

	@Override
	public final Object connectedOutput() {
		return tcpStream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public SocketChannel delegate() {
		return (SocketChannel) tcpStream.delegate();
	}

	public void doAddCookie(String name, Cookie cookie) {
		doAddHeader(ResponseHeaders.SET_COOKIE,
					ServerCookieEncoder.STRICT.encode(new Cookies.NettyWriteCookie(cookie)));
	}

	// REQUEST contract

	public void doAddResponseCookie(String name, Cookie cookie) {
		doAddResponseHeader(ResponseHeaders.SET_COOKIE,
					ServerCookieEncoder.STRICT.encode(new Cookies.NettyWriteCookie(cookie)));
	}

	public void doResponseStatus(Status status) {
		this.nettyResponse.setStatus(HttpResponseStatus.valueOf(status.getCode()));
	}

	@Override
	public String getName() {
		if(isWebsocket()){
			return Method.WS.getName()+":"+uri();
		}

		return  method().getName()+":"+uri();
	}

	public HttpRequest getNettyRequest() {
		return nettyRequest;
	}

	public HttpResponse getNettyResponse() {
		return nettyResponse;
	}

	void setNettyResponse(HttpResponse nettyResponse) {
		this.nettyResponse = nettyResponse;
		this.responseHeaders = new NettyHttpResponseHeaders(this.nettyResponse);
	}

	/**
	 * Register an HTTP request header
	 * @param name Header name
	 * @param value Header content
	 * @return this
	 */
	@Override
	public HttpChannel<Buffer, Buffer> header(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public NettyHttpHeaders headers() {
		return this.headers;
	}

	@Override
	public Flux<Buffer> input() {
		return this;
	}

	// RESPONSE contract

	@Override
	public boolean isKeepAlive() {
		return headers.isKeepAlive();
	}

	@Override
	public boolean isStarted() {
		return tcpStream.isStarted();
	}

	@Override
	public boolean isTerminated() {
		return tcpStream.isTerminated();
	}

	@Override
	public boolean isWebsocket() {
		String isWebsocket = headers.get(HttpHeaders.UPGRADE);
		return isWebsocket != null && isWebsocket.toLowerCase().equals("websocket");
	}

	@Override
	public HttpChannel<Buffer, Buffer> keepAlive(boolean keepAlive) {
		responseHeaders.keepAlive(keepAlive);
		return this;
	}

	@Override
	public Method method() {
		return new Method(this.nettyRequest.getMethod().name());
	}

	@Override
	public ConsumerSpec on() {
		return tcpStream.on();
	}

	/**
	 * Read URI param from the given key
	 * @param key matching key
	 * @return the resolved parameter for the given key name
	 */
	@Override
	public Object param(String key) {
		Map<String, Object> params = null;
		if (paramsResolver != null) {
			params = this.paramsResolver.apply(uri());
		}
		return null != params ? params.get(key) : null;
	}

	/**
	 * Read all URI params
	 * @return a map of resolved parameters against their matching key name
	 */
	@Override
	public Map<String, Object> params() {
		return null != paramsResolver ? paramsResolver.apply(uri()) : null;
	}

	/**
	 *
	 * @param headerResolver
	 */
	@Override
	public HttpChannel<Buffer, Buffer> paramsResolver(
			Function<? super String, Map<String, Object>> headerResolver) {
		this.paramsResolver = headerResolver;
		return this;
	}

	@Override
	public Protocol protocol() {
		HttpVersion version = this.nettyRequest.getProtocolVersion();
		if (version.equals(HttpVersion.HTTP_1_0)) {
			return Protocol.HTTP_1_0;
		} else if (version.equals(HttpVersion.HTTP_1_1)) {
			return Protocol.HTTP_1_1;
		}
		throw new IllegalStateException(version.protocolName() + " not supported");
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return tcpStream.remoteAddress();
	}

	/**
	 * Define the response HTTP header for the given key
	 * @param name the HTTP response header key to override
	 * @param value the HTTP response header content
	 * @return this
	 */
	@Override
	public HttpChannel<Buffer, Buffer> responseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doResponseHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public NettyHttpResponseHeaders responseHeaders() {
		return this.responseHeaders;
	}

	@Override
	public Status responseStatus() {
		return Status.valueOf(this.nettyResponse.status().code());
	}

	/**
	 * Set the response status to an outgoing response
	 * @param status the status to define
	 * @return this
	 */
	@Override
	public HttpChannel<Buffer, Buffer> responseStatus(Status status) {
		if (statusAndHeadersSent == 0) {
			doResponseStatus(status);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	/**
	 * @return the Transfer setting SSE for this http connection (e.g. event-stream)
	 */
	@Override
	public HttpChannel<Buffer, Buffer> sse() {
		return transfer(Transfer.EVENT_STREAM);
	}

	@Override
	public void subscribe(final Subscriber<? super Buffer> subscriber) {
		// Handle the 'Expect: 100-continue' header if necessary.
		// TODO: Respond with 413 Request Entity Too Large
		//   and discard the traffic or close the connection.
		//       No need to notify the upstream handlers - just log.
		//       If decoding a response, just throw an error.
		if (HttpUtil.is100ContinueExpected(nettyRequest)) {
			tcpStream.delegate().writeAndFlush(CONTINUE).addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (!future.isSuccess()) {
						subscriber.onError(future.cause());
					} else {
						tcpStream.subscribe(subscriber);
					}
				}
			});
		} else {
			tcpStream.subscribe(subscriber);
		}
	}

	// REQUEST contract

	@Override
	public Transfer transfer() {
		if ("chunked".equals(this.headers.get(ResponseHeaders.TRANSFER_ENCODING))) {
			Assert.isTrue(Protocol.HTTP_1_1.equals(protocol()));
			return Transfer.CHUNKED;
		} else if (this.headers.get(ResponseHeaders.TRANSFER_ENCODING) == null) {
			return Transfer.NON_CHUNKED;
		}
		throw new IllegalStateException("Can't determine a valide transfer based on headers and protocol");
	}

	@Override
	public HttpChannel<Buffer, Buffer> transfer(Transfer transfer) {
		switch (transfer) {
			case EVENT_STREAM:
				this.responseHeader(ResponseHeaders.CONTENT_TYPE, "text/event-stream");
			case CHUNKED:
				Assert.isTrue(Protocol.HTTP_1_1.equals(protocol()));
				this.responseHeader(ResponseHeaders.TRANSFER_ENCODING, "chunked");
				break;
			case NON_CHUNKED:
				this.responseHeaders().remove(ResponseHeaders.TRANSFER_ENCODING);
		}
		return this;
	}

	@Override
	public String uri() {
		return this.nettyRequest.getUri();
	}

	@Override
	public Mono<Void> writeBufferWith(final Publisher<? extends Buffer> dataStream) {
		return new PostBufferWritePublisher(dataStream);
	}

	/**
	 * Flush the headers if not sent. Might be useful for the case
	 * @return Stream to signal error or successful write to the client
	 */
	@Override
	public Mono<Void> writeHeaders() {
		if (statusAndHeadersSent == 0) {
			return new PostHeaderWritePublisher();
		}
		else {
			return Mono.empty();
		}
	}

	@Override
	public Mono<Void> writeWith(final Publisher<? extends Buffer> source) {
		return new PostWritePublisher(source);
	}

	// RESPONSE contract

	protected Mono<Void> writeWithAfterHeaders(Publisher<? extends Buffer> writer) {
		return tcpStream.writeWith(writer);
	}

	protected Publisher<Void> writeWithBufferAfterHeaders(Publisher<? extends Buffer> s) {
		return tcpStream.writeBufferWith(s);
	}

	protected void doHeader(String name, String value) {
		this.headers.set(name, value);
	}

	protected void doAddHeader(String name, String value) {
		this.headers.add(name, value);
	}

	protected void doResponseHeader(String name, String value) {
		this.responseHeaders.set(name, value);
	}

	protected void doAddResponseHeader(String name, String value) {
		this.responseHeaders.add(name, value);
	}

	protected abstract void doSubscribeHeaders(Subscriber<? super Void> s);

	protected boolean markHeadersAsFlushed() {
		return HEADERS_SENT.compareAndSet(this, 0, 1);
	}
	protected final static AtomicIntegerFieldUpdater<NettyHttpChannel> HEADERS_SENT =
			AtomicIntegerFieldUpdater.newUpdater(NettyHttpChannel.class, "statusAndHeadersSent");
	private static final FullHttpResponse CONTINUE =
			new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);

	private class PostWritePublisher extends Mono<Void> implements Receiver, Loopback {

		private final Publisher<? extends Buffer> source;

		public PostWritePublisher(Publisher<? extends Buffer> source) {
			this.source = source;
		}

		@Override
		public Object connectedInput() {
			return NettyHttpChannel.this;
		}

		@Override
		public Object connectedOutput() {
			return NettyHttpChannel.this;
		}

		@Override
		public void subscribe(final Subscriber<? super Void> s) {
			if(markHeadersAsFlushed()){
				doSubscribeHeaders(new PostHeaderWriteSubscriber(s));
			}
			else{
				writeWithAfterHeaders(source).subscribe(s);
			}
		}

		@Override
		public Object upstream() {
			return source;
		}

		private class PostHeaderWriteSubscriber implements Subscriber<Void>, Receiver, Producer {

			private final Subscriber<? super Void> s;
			private       Subscription             subscription;

			public PostHeaderWriteSubscriber(Subscriber<? super Void> s) {
				this.s = s;
			}

			@Override
			public Subscriber downstream() {
				return s;
			}

			@Override
			public void onComplete() {
				this.subscription = null;
				writeWithAfterHeaders(source).subscribe(s);
			}

			@Override
			public void onError(Throwable t) {
				this.subscription = null;
				s.onError(t);
			}

			@Override
			public void onNext(Void aVoid) {
				//Ignore
			}

			@Override
			public void onSubscribe(Subscription sub) {
				this.subscription = sub;
				sub.request(Long.MAX_VALUE);
			}

			@Override
			public Object upstream() {
				return subscription;
			}
		}
	}

	private class PostHeaderWritePublisher extends Mono<Void> implements Loopback {

		@Override
		public Object connectedInput() {
			return NettyHttpChannel.this;
		}

		@Override
		public Object connectedOutput() {
			return NettyHttpChannel.this;
		}

		@Override
		public void subscribe(Subscriber<? super Void> s) {
			if (markHeadersAsFlushed()) {
				doSubscribeHeaders(s);
			}
			else {
				EmptySubscription.error(s, new IllegalStateException("Status and headers already sent"));
			}
		}
	}

	private class PostBufferWritePublisher extends Mono<Void> implements Receiver, Loopback {

		private final Publisher<? extends Buffer> dataStream;

		public PostBufferWritePublisher(Publisher<? extends Buffer> dataStream) {
			this.dataStream = dataStream;
		}

		@Override
		public Object connectedInput() {
			return NettyHttpChannel.this;
		}

		@Override
		public Object connectedOutput() {
			return NettyHttpChannel.this;
		}

		@Override
		public void subscribe(final Subscriber<? super Void> s) {
			if(markHeadersAsFlushed()){
				doSubscribeHeaders(new PostHeaderWriteBufferSubscriber(s));
			}
			else{
				writeWithBufferAfterHeaders(dataStream).subscribe(s);
			}
		}

		@Override
		public Object upstream() {
			return dataStream;
		}

		private class PostHeaderWriteBufferSubscriber implements Subscriber<Void>, Producer, Receiver {

			private final Subscriber<? super Void> s;
			private Subscription subscription;

			public PostHeaderWriteBufferSubscriber(Subscriber<? super Void> s) {
				this.s = s;
			}

			@Override
			public Subscriber downstream() {
				return s;
			}

			@Override
			public void onComplete() {
				this.subscription = null;
				writeWithBufferAfterHeaders(dataStream).subscribe(s);
			}

			@Override
			public void onError(Throwable t) {
				this.subscription = null;
				s.onError(t);
			}

			@Override
			public void onNext(Void aVoid) {
				//Ignore
			}

			@Override
			public void onSubscribe(Subscription sub) {
				this.subscription = sub;
				sub.request(Long.MAX_VALUE);
			}

			@Override
			public Object upstream() {
				return subscription;
			}
		}
	}

}
