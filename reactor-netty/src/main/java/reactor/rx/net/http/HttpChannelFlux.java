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

package reactor.rx.net.http;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.timer.Timer;
import reactor.io.ipc.RemoteFluxHandler;
import reactor.io.netty.http.BaseHttpChannel;
import reactor.io.netty.http.HttpChannel;
import reactor.io.netty.http.model.Cookie;
import reactor.io.netty.http.model.HttpHeaders;
import reactor.io.netty.http.model.Method;
import reactor.io.netty.http.model.Protocol;
import reactor.io.netty.http.model.ResponseHeaders;
import reactor.io.netty.http.model.Status;
import reactor.io.netty.http.model.Transfer;
import reactor.rx.net.ChannelFlux;

/**
 * A Request/Response {@link ChannelFlux} extension that provides for several helpers to
 * control HTTP behavior and observe its metadata.
 * @author Stephane maldini
 * @since 2.5
 */
public class HttpChannelFlux<IN, OUT> extends ChannelFlux<IN, OUT> implements HttpChannel<IN, OUT> {

	private final HttpChannel<IN, OUT> actual;

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> HttpChannelFlux<IN, OUT> wrap(final HttpChannel<IN, OUT> actual, Timer timer, long prefetch){
		return new HttpChannelFlux<>(actual, timer, prefetch);
	}

	/**
	 *
	 * @param actual
	 * @param timer
	 * @param prefetch
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> RemoteFluxHandler<IN, OUT, HttpChannel<IN, OUT>> wrapHttp(
			final RemoteFluxHandler<IN, OUT, HttpChannelFlux<IN, OUT>> actual, final Timer timer, final long prefetch){

		if(actual == null) return null;

		return stream -> actual.apply(wrap(stream, timer, prefetch));
	}

	protected HttpChannelFlux(HttpChannel<IN, OUT> actual, Timer timer, long prefetch) {
		super(actual, timer, prefetch);
		this.actual = actual;
	}

	// REQUEST contract

	/**
	 * @see BaseHttpChannel#params()
	 */
	public final Map<String, Object> params() {
		return actual.params();
	}

	/**
	 * Read URI param from the given key
	 * @param key matching key
	 * @return the resolved parameter for the given key name
	 */
	public final Object param(String key) {
		return actual.param(key);
	}

	/**
	 * @return Resolved HTTP request headers
	 */
	public HttpHeaders headers(){
		return actual.headers();
	}

	/**
	 * Register an HTTP request header
	 * @param name Header name
	 * @param value Header content
	 * @return this
	 */
	public final HttpChannelFlux<IN, OUT> header(String name, String value) {
		actual.header(name, value);
		return this;
	}

	/**
	 * Is the request keepAlive
	 * @return is keep alive
	 */
	public boolean isKeepAlive(){
		return actual.isKeepAlive();
	}

	/**
	 * set the request keepAlive if true otherwise remove the existing connection keep
	 * alive header
	 * @return is keep alive
	 */
	public HttpChannelFlux<IN, OUT> keepAlive(boolean keepAlive){
		actual.keepAlive(keepAlive);
		return this;
	}


	/**
	 * Accumulate a Request Header using the given name and value, appending ";" for each
	 * new value
	 * @return this
	 */
	public HttpChannelFlux<IN, OUT> addHeader(String name, String value) {
		actual.addHeader(name, value);
		return this;
	}

	@Override
	public Protocol protocol(){
		return actual.protocol();
	}

	@Override
	public String uri(){
		return actual.uri();
	}

	@Override
	public Method method(){
		return actual.method();
	}

	@Override
	public Map<String, Set<Cookie>> cookies() {
		return actual.cookies();
	}

	@Override
	public HttpChannelFlux<IN, OUT> addCookie(String name, Cookie cookie) {
		actual.addCookie(name, cookie);
		return this;
	}

	// RESPONSE contract

	@Override
	public HttpChannelFlux<IN, OUT> addResponseCookie(String name, Cookie cookie) {
		actual.addResponseCookie(name, cookie);
		return this;
	}

	@Override
	public Status responseStatus(){
		return actual.responseStatus();
	}

	@Override
	public HttpChannelFlux<IN, OUT> responseStatus(Status status) {
		actual.responseStatus(status);
		return this;
	}

	@Override
	public ResponseHeaders responseHeaders(){
		return actual.responseHeaders();
	}

	@Override
	public final HttpChannelFlux<IN, OUT> responseHeader(String name, String value) {
		actual.responseHeader(name, value);
		return this;
	}

	@Override
	public HttpChannelFlux<IN, OUT> addResponseHeader(String name, String value) {
		actual.addResponseHeader(name, value);
		return this;
	}

	@Override
	public Mono<Void> writeHeaders() {
		return actual.writeHeaders();
	}

	@Override
	public HttpChannelFlux<IN, OUT> sse() {
		return transfer(Transfer.EVENT_STREAM);
	}

	@Override
	public Transfer transfer(){
		return actual.transfer();
	}

	@Override
	public HttpChannelFlux<IN, OUT> transfer(Transfer transfer){
		actual.transfer(transfer);
		return this;
	}

	@Override
	public boolean isWebsocket(){
		return actual.isWebsocket();
	}

	@Override
	public final Mono<Void> writeWith(final Publisher<? extends OUT> source) {
		return actual.writeWith(source);
	}

	@Override
	public HttpChannel<IN, OUT> paramsResolver(
			Function<? super String, Map<String, Object>> headerResolver) {
		return actual.paramsResolver(headerResolver);
	}
}
