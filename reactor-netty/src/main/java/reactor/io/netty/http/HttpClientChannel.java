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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.cookie.Cookie;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;
import reactor.core.util.Exceptions;
import reactor.io.netty.common.MonoChannelFuture;

/**
 * @author Stephane Maldini
 */
final class HttpClientChannel extends NettyHttpChannel
		implements HttpClientResponse, HttpClientRequest {

	final String[] redirectedFrom;

	boolean redirectable;
	Cookies cookies;

	public HttpClientChannel(long prefetch,
			io.netty.channel.Channel ioChannel,
			String[] redirects) {
		super(prefetch, ioChannel, null);
		redirectedFrom = redirects == null ? EMPTY_REDIRECTIONS : redirects;
	}

	@Override
	void setNettyResponse(HttpResponse nettyResponse) {
		super.setNettyResponse(nettyResponse);
		this.cookies = Cookies.newClientResponseHolder(responseHeaders());
	}

	@Override
	public boolean isWebsocket() {
		return delegate().pipeline()
		                 .get(NettyWebSocketClientHandler.class) != null;
	}

	@Override
	protected void doSubscribeHeaders(Subscriber<? super Void> s) {
		MonoChannelFuture.from(delegate().writeAndFlush(getNettyRequest()))
		                 .subscribe(s);
	}

	@Override
	public Mono<Void> upgradeToWebsocket(String protocols, boolean textPlain) {
		ChannelPipeline pipeline = delegate().pipeline();
		NettyWebSocketClientHandler handler;

		URI uri;
		try {
			uri = new URI(HttpClient.parseURL(remoteAddress(), uri(), true));
		}
		catch (URISyntaxException e) {
			throw Exceptions.bubble(e);
		}

		pipeline.addLast(new HttpObjectAggregator(8192));
		handler = pipeline.remove(NettyHttpClientHandler.class)
		                  .withWebsocketSupport(uri, protocols, textPlain);

		if (handler != null) {
			pipeline.addLast(handler);
			return MonoChannelFuture.from(handler.handshakerResult);
		}
		return Mono.error(new IllegalStateException("Failed to upgrade to websocket"));
	}

	@Override
	public Map<CharSequence, Set<Cookie>> cookies() {
		return cookies.getCachedCookies();
	}

	@Override
	public HttpClientRequest followRedirect() {
		redirectable = true;
		return this;
	}

	@Override
	public boolean isFollowRedirect() {
		return redirectable && redirectedFrom.length <= MAX_REDIRECTS;
	}

	@Override
	public String[] redirectedFrom() {
		return Arrays.asList(redirectedFrom)
		             .toArray(new String[redirectedFrom.length]);
	}

	static final int      MAX_REDIRECTS      = 50;
	static final String[] EMPTY_REDIRECTIONS = new String[0];
}
