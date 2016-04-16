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
import io.netty.handler.codec.http.cookie.Cookie;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.io.buffer.Buffer;
import reactor.io.netty.common.NettyChannel;

/**
 *
 * An Http Reactive Channel with several accessor related to HTTP flow : headers, params,
 * URI, method, websocket...
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface HttpChannel extends NettyChannel, HttpConnection {

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
	 * @return Resolved HTTP request headers
	 */
	HttpHeaders headers();

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
	HttpChannel responseTransfer(boolean chunked);

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
	 * @return
	 */
	Mono<Void> sendHeaders();

	/**
	 *
	 * @return
	 */
	HttpChannel sse();

	/**
	 *
	 * @param status
	 * @return
	 */
	HttpChannel status(HttpResponseStatus status);

	/**
	 *
	 * @param status
	 * @return
	 */
	default HttpChannel status(int status){
		return status(HttpResponseStatus.valueOf(status));
	}

	/**
	 * @return the resolved HTTP Response Status
	 */
	HttpResponseStatus status();

}
