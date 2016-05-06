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
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AsciiString;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.Subscribers;
import reactor.core.util.EmptySubscription;

import static reactor.io.netty.http.HttpClient.HTTPS_SCHEME;

/**
 * @author Stephane Maldini
 */
final class MonoClientRequest extends Mono<HttpInbound> {

	private HttpClient                                                client;
	final   URI                                                       currentURI;
	final   HttpMethod                                                method;
	final   Function<? super HttpOutbound, ? extends Publisher<Void>> handler;

	static final AsciiString ALL = new AsciiString("*/*");

	public MonoClientRequest(HttpClient client,
			URI currentURI,
			HttpMethod method, Function<? super HttpOutbound, ? extends Publisher<Void>> handler) {
		this.client = client;
		this.currentURI = currentURI;
		this.method = method;
		this.handler = handler;
	}

	@Override
	public void subscribe(final Subscriber<? super HttpInbound> subscriber) {
		client.doStart(currentURI, c -> {
			try {
				URI uri = currentURI;
				NettyHttpChannel ch = (NettyHttpChannel) c;
				ch.getNettyRequest()
				  .setUri(uri.getPath() +
						  (uri.getQuery() == null ? "" : "?" + uri.getRawQuery()))
				  .setMethod(method)
				  .headers()
				  .add(HttpHeaderNames.HOST, uri.getHost())
				  .add(HttpHeaderNames.ACCEPT, ALL);

				if(method == HttpMethod.GET ||
						method == HttpMethod.HEAD ||
						method == HttpMethod.DELETE ){
					ch.removeTransferEncodingChunked();
				}

				ch.delegate()
				  .pipeline()
				  .fireUserEventTriggered(new NettyHttpClientHandler.ChannelInputSubscriberEvent(subscriber));

				if (handler != null) {
					return handler.apply(ch);
				}
				else {
					HttpUtil.setTransferEncodingChunked(ch.getNettyResponse(), false);
					return ch.sendHeaders();
				}
			}
			catch (Throwable t) {
				return Mono.error(t);
			}
		})
		      .subscribe(null, reason -> EmptySubscription.error(subscriber, reason));
	}
}
