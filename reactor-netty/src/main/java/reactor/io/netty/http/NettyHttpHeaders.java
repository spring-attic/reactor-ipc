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
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import reactor.io.netty.http.model.HttpHeaders;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
final class NettyHttpHeaders implements HttpHeaders {

	private final HttpRequest                             nettyRequest;
	private final io.netty.handler.codec.http.HttpHeaders nettyHeaders;

	NettyHttpHeaders(HttpRequest nettyRequest) {
		this.nettyRequest = nettyRequest;
		this.nettyHeaders = nettyRequest.headers();
	}

	public io.netty.handler.codec.http.HttpHeaders delegate(){
		return nettyHeaders;
	}

	@Override
	public HttpHeaders add(CharSequence name, Object value) {
		nettyHeaders.add(name, value);
		return this;
	}

	@Override
	public HttpHeaders add(CharSequence name, Iterable<?> values) {
		nettyHeaders.add(name, values);
		return this;
	}

	@Override
	public HttpHeaders clear() {
		nettyHeaders.clear();
		return this;
	}

	@Override
	public HttpHeaders remove(CharSequence name) {
		nettyHeaders.remove(name);
		return this;
	}

	@Override
	public HttpHeaders removeTransferEncodingChunked() {
		HttpUtil.setTransferEncodingChunked(nettyRequest, false);
		return this;
	}

	@Override
	public HttpHeaders set(CharSequence name, Object value) {
		nettyHeaders.set(name, value);
		return this;
	}

	@Override
	public HttpHeaders set(CharSequence name, Iterable<?> values) {
		nettyHeaders.set(name, values);
		return this;
	}

	@Override
	public HttpHeaders contentLength(long length) {
		HttpUtil.setContentLength(nettyRequest, length);
		return this;
	}

	@Override
	public HttpHeaders host(CharSequence value) {
		set(HttpHeaders.HOST, value);
		return this;
	}

	@Override
	public HttpHeaders keepAlive(boolean keepAlive) {
		HttpUtil.setKeepAlive(nettyRequest, keepAlive);
		return this;
	}

	@Override
	public boolean isKeepAlive() {
		return HttpUtil.isKeepAlive(nettyRequest);
	}

	@Override
	public HttpHeaders transferEncodingChunked() {
		HttpUtil.setTransferEncodingChunked(nettyRequest, true);
		return this;
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
	public Long getTimeMillis(){
		return nettyHeaders.getTimeMillis(HttpHeaderNames.DATE);
	}

	@Override
	public Long getTimeMillis(CharSequence name){
		return nettyHeaders.getTimeMillis(name);
	}

	@Override
	public HttpHeaders timeMillis(Long timeMillis) {
		set(HttpHeaderNames.DATE, timeMillis);
		return this;
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



}
