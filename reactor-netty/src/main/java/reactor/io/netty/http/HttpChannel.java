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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.Cookie;
import reactor.core.publisher.Mono;
import reactor.io.netty.common.NettyChannel;

/**
 *
 * An Http Reactive Channel with several accessor related to HTTP flow : headers, params,
 * URI, method, websocket...
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface HttpChannel extends NettyChannel {

	String WS_SCHEME    = "ws";
	String WSS_SCHEME   = "wss";
	String HTTP_SCHEME  = "http";
	String HTTPS_SCHEME = "https";

	/**
	 * add the passed cookie
	 * @return this
	 */
	HttpChannel addCookie(Cookie cookie);

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	HttpChannel addHeader(CharSequence name, CharSequence value);

	/**
	 * add the passed cookie
	 * @return this
	 */
	HttpChannel addResponseCookie(Cookie cookie);

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	HttpChannel addResponseHeader(CharSequence name, CharSequence value);

	/**
	 * @return Resolved HTTP cookies
	 */
	Map<CharSequence, Set<Cookie>> cookies();

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	HttpChannel header(CharSequence name, CharSequence value);

	/**
	 * @return Resolved HTTP request headers
	 */
	HttpHeaders headers();

	/**
	 * Is the request keepAlive
	 * @return is keep alive
	 */
	boolean isKeepAlive();

	/**
	 *
	 * @return
	 */
	boolean isWebsocket();

	/**
	 * set the request keepAlive if true otherwise remove the existing connection keep
	 * alive header
	 * @return is keep alive
	 */
	HttpChannel keepAlive(boolean keepAlive);

	/**
	 * @return the resolved request method (HTTP 1.1 etc)
	 */
	HttpMethod method();

	/**
	 *
	 * @param key
	 * @return
	 */
	Object param(CharSequence key);

	/**
	 *
	 * @return
	 */
	Map<String, Object> params();

	/**
	 *
	 * @param headerResolver
	 * @return
	 */
	HttpChannel paramsResolver(Function<? super String, Map<String, Object>> headerResolver);

	/**
	 *
	 */
	HttpChannel removeTransferEncodingChunked();

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	HttpChannel responseHeader(CharSequence name, CharSequence value);

	/**
	 * @return the resolved response HTTP headers
	 */
	HttpHeaders responseHeaders();

	/**
	 * @return the resolved HTTP Response Status
	 */
	HttpResponseStatus responseStatus();

	/**
	 *
	 * @param status
	 * @return
	 */
	HttpChannel responseStatus(HttpResponseStatus status);

	/**
	 *
	 * @param status
	 * @return
	 */
	default HttpChannel responseStatus(int status){
		return responseStatus(HttpResponseStatus.valueOf(status));
	}

	/**
	 *
	 * @return
	 */
	HttpChannel sse();

	/**
	 * @return the resolved target address
	 */
	String uri();

	/**
	 * @return the resolved request version (HTTP 1.1 etc)
	 */
	HttpVersion version();

	/**
	 *
	 * @return
	 */
	Mono<Void> sendHeaders();
}
