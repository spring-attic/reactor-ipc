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

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import reactor.io.netty.http.model.ResponseHeaders;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
final class NettyHttpResponseHeaders implements ResponseHeaders {

	private final HttpResponse nettyResponse;
	private final HttpHeaders nettyHeaders;


	NettyHttpResponseHeaders(HttpResponse nettyResponse) {
		this.nettyResponse = nettyResponse;
		this.nettyHeaders = nettyResponse.headers();
	}

	public HttpHeaders delegate(){
		return nettyHeaders;
	}

	@Override
	public boolean contains(CharSequence name) {
		return this.nettyHeaders.contains(name);
	}

	@Override
	public boolean contains(CharSequence name, CharSequence value, boolean ignoreCaseValue) {
		return this.nettyHeaders.contains(name, value, ignoreCaseValue);
	}

	@Override
	public List<Map.Entry<String, String>> entries() {
		return this.nettyHeaders.entries();
	}

	@Override
	public String get(CharSequence name) {
		return this.nettyHeaders.get(name);
	}

	@Override
	public List<String> getAll(CharSequence name) {
		return this.nettyHeaders.getAll(name);
	}

	@Override
	public Long getTimeMillis() {
		return nettyHeaders.getTimeMillis(HttpHeaderNames.DATE);
	}

	@Override
	public String getHost() {
		return get(HttpHeaderNames.HOST);
	}

	@Override
	public boolean isEmpty() {
		return this.nettyHeaders.isEmpty();
	}

	@Override
	public Set<String> names() {
		return this.nettyHeaders.names();
	}

	@Override
	public long getContentLength() {
		return 0;
	}

	@Override
	public ResponseHeaders add(CharSequence name, Object value) {
		this.nettyHeaders.add(name, value);
		return this;
	}

	@Override
	public ResponseHeaders add(CharSequence name, Iterable<?> values) {
		this.nettyHeaders.add(name, values);
		return this;
	}

	@Override
	public ResponseHeaders clear() {
		this.nettyHeaders.clear();
		return this;
	}

	@Override
	public ResponseHeaders remove(CharSequence name) {
		this.nettyHeaders.remove(name);
		return this;
	}

	@Override
	public ResponseHeaders removeTransferEncodingChunked() {
		HttpUtil.setTransferEncodingChunked(this.nettyResponse, false);
		return this;
	}

	@Override
	public ResponseHeaders set(CharSequence name, Object value) {
		this.nettyHeaders.set(name, value);
		return this;
	}

	@Override
	public ResponseHeaders set(CharSequence name, Iterable<?> values) {
		this.nettyHeaders.set(name, values);
		return this;
	}

	@Override
	public ResponseHeaders contentLength(long length) {
		HttpUtil.setContentLength(this.nettyResponse, length);
		return this;
	}

	@Override
	public ResponseHeaders host(CharSequence value) {
		set(HttpHeaderNames.HOST, value);
		return this;
	}

	@Override
	public boolean isKeepAlive(){
		return HttpUtil.isKeepAlive(this.nettyResponse);
	}

	@Override
	public ResponseHeaders keepAlive(boolean keepAlive) {
		HttpUtil.setKeepAlive(this.nettyResponse, keepAlive);
		return this;
	}

	@Override
	public ResponseHeaders transferEncodingChunked() {
		HttpUtil.setTransferEncodingChunked(this.nettyResponse, true);
		return this;
	}

	@Override
	public Long getTimeMillis(CharSequence name) {
		return nettyHeaders.getTimeMillis(name);
	}

	@Override
	public ResponseHeaders timeMillis(Long timeMillis) {
		set(HttpHeaderNames.DATE, timeMillis);
		return this;
	}
}
