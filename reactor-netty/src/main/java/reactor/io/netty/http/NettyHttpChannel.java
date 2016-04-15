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
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.util.AsciiString;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.state.Completable;
import reactor.core.util.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.netty.tcp.TcpChannel;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
abstract class NettyHttpChannel extends Flux<Buffer> implements HttpChannel, Loopback, Completable {

	final static AsciiString EVENT_STREAM = new AsciiString("text/event-stream");

	final TcpChannel  tcpStream;
	final boolean     client;
	final HttpRequest nettyRequest;
	final HttpHeaders headers;
	HttpResponse nettyResponse;
	HttpHeaders  responseHeaders;
	volatile int statusAndHeadersSent = 0;
	Function<? super String, Map<String, Object>> paramsResolver;

	public NettyHttpChannel(TcpChannel tcpStream,
	                        HttpRequest request
	) {
		if (request == null) {
			client = true;
			nettyRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		}
		else {
			client = false;
			nettyRequest = request;
		}

		this.tcpStream = tcpStream;
		this.nettyResponse = new DefaultHttpResponse(nettyRequest.protocolVersion(), HttpResponseStatus.OK);
		this.headers = nettyRequest.headers();
		this.responseHeaders = nettyResponse.headers();
		this.responseHeader(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
	}

	@Override
	public HttpChannel addCookie(CharSequence name, Cookie cookie) {
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
	public HttpChannel addHeader(CharSequence name, CharSequence value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public HttpChannel addResponseCookie(CharSequence name, Cookie cookie) {
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
	public HttpChannel addResponseHeader(CharSequence name, CharSequence value) {
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

	@Override
	public String getName() {
		if(isWebsocket()){
			return "ws:" + uri();
		}

		return method().name() + ":" + uri();
	}

	// REQUEST contract

	/**
	 * Register an HTTP request header
	 * @param name Header name
	 * @param value Header content
	 * @return this
	 */
	@Override
	public HttpChannel header(CharSequence name, CharSequence value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public HttpHeaders headers() {
		return this.headers;
	}

	@Override
	public Flux<Buffer> receive() {
		return this;
	}

	@Override
	public boolean isKeepAlive() {
		return HttpUtil.isKeepAlive(nettyRequest);
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
		String isWebsocket = headers.get(HttpHeaderNames.UPGRADE);
		return isWebsocket != null && isWebsocket.toLowerCase().equals("websocket");
	}

	@Override
	public HttpChannel keepAlive(boolean keepAlive) {
		HttpUtil.setKeepAlive(nettyResponse, keepAlive);
		return this;
	}

	@Override
	public HttpMethod method() {
		return nettyRequest.method();
	}

	// RESPONSE contract

	@Override
	public Lifecycle on() {
		return tcpStream.on();
	}

	/**
	 * Read URI param from the given key
	 * @param key matching key
	 * @return the resolved parameter for the given key name
	 */
	@Override
	public Object param(CharSequence key) {
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
	public HttpChannel paramsResolver(
			Function<? super String, Map<String, Object>> headerResolver) {
		this.paramsResolver = headerResolver;
		return this;
	}

	@Override
	public HttpVersion version() {
		HttpVersion version = this.nettyRequest.protocolVersion();
		if (version.equals(HttpVersion.HTTP_1_0)) {
			return HttpVersion.HTTP_1_0;
		} else if (version.equals(HttpVersion.HTTP_1_1)) {
			return HttpVersion.HTTP_1_1;
		}
		throw new IllegalStateException(version.protocolName() + " not supported");
	}

	@Override
	public HttpChannel removeTransferEncodingChunked() {
		if(client) {
			HttpUtil.setTransferEncodingChunked(nettyRequest, false);
		}
		else {
			HttpUtil.setTransferEncodingChunked(nettyResponse, false);
		}
		return this;
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
	public HttpChannel responseHeader(CharSequence name, CharSequence value) {
		if (statusAndHeadersSent == 0) {
			doResponseHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	@Override
	public HttpHeaders responseHeaders() {
		return this.responseHeaders;
	}

	@Override
	public HttpResponseStatus responseStatus() {
		return HttpResponseStatus.valueOf(this.nettyResponse.status()
		                                                    .code());
	}

	/**
	 * Set the response status to an outgoing response
	 * @param status the status to define
	 * @return this
	 */
	@Override
	public HttpChannel responseStatus(HttpResponseStatus status) {
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
	public HttpChannel sse() {
		return header(HttpHeaderNames.CONTENT_TYPE, EVENT_STREAM);
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

	@Override
	public String uri() {
		return this.nettyRequest.uri();
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
	public Mono<Void> send(final Publisher<? extends Buffer> source) {
		return new PostWritePublisher(source);
	}

	// REQUEST contract

	protected Mono<Void> sendAfterHeaders(Publisher<? extends Buffer> writer) {
		return tcpStream.send(writer);
	}

	protected Publisher<Void> sendBufferAfterHeaders(Publisher<? extends Buffer> s) {
		return tcpStream.send(s);
	}

	protected void doHeader(CharSequence name, CharSequence value) {
		this.headers.set(name, value);
	}

	protected void doAddHeader(CharSequence name, CharSequence value) {
		this.headers.add(name, value);
	}

	protected void doResponseHeader(CharSequence name, CharSequence value) {
		this.responseHeaders.set(name, value);
	}

	protected void doAddResponseHeader(CharSequence name, CharSequence value) {
		this.responseHeaders.add(name, value);
	}

	// RESPONSE contract

	protected abstract void doSubscribeHeaders(Subscriber<? super Void> s);

	protected boolean markHeadersAsFlushed() {
		return HEADERS_SENT.compareAndSet(this, 0, 1);
	}

	void doAddCookie(CharSequence name, Cookie cookie) {
		doAddHeader(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
	}

	void doAddResponseCookie(CharSequence name, Cookie cookie) {
		doAddResponseHeader(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
	}

	void doResponseStatus(HttpResponseStatus status) {
		this.nettyResponse.setStatus(HttpResponseStatus.valueOf(status.code()));
	}

	HttpRequest getNettyRequest() {
		return nettyRequest;
	}

	HttpResponse getNettyResponse() {
		return nettyResponse;
	}

	void setNettyResponse(HttpResponse nettyResponse) {
		this.nettyResponse = nettyResponse;
		this.responseHeaders = nettyResponse.headers();
	}
	protected final static AtomicIntegerFieldUpdater<NettyHttpChannel> HEADERS_SENT =
			AtomicIntegerFieldUpdater.newUpdater(NettyHttpChannel.class, "statusAndHeadersSent");
	static final           FullHttpResponse                            CONTINUE     =
			new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);

	class PostWritePublisher extends Mono<Void> implements Receiver, Loopback {

		final Publisher<? extends Buffer> source;

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
				sendAfterHeaders(source).subscribe(s);
			}
		}

		@Override
		public Object upstream() {
			return source;
		}

		class PostHeaderWriteSubscriber implements Subscriber<Void>, Receiver, Producer {

			final Subscriber<? super Void> s;
			Subscription subscription;

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
				sendAfterHeaders(source).subscribe(s);
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

	class PostHeaderWritePublisher extends Mono<Void> implements Loopback {

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
}
