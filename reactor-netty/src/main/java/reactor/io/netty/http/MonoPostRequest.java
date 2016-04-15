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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;
import reactor.core.subscriber.Subscribers;
import reactor.core.util.EmptySubscription;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFluxHandler;

/**
 * @author Stephane Maldini
 */
final class MonoPostRequest extends Mono<HttpChannel> {

	private HttpClient                                      client;
	final   URI                                             currentURI;
	final   HttpMethod                                      method;
	final   ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler;

	public MonoPostRequest(HttpClient client,
			URI currentURI,
			HttpMethod method,
			ChannelFluxHandler<Buffer, Buffer, HttpChannel> handler) {
		this.client = client;
		this.currentURI = currentURI;
		this.method = method;
		this.handler = handler;
	}

	@Override
	public void subscribe(final Subscriber<? super HttpChannel> subscriber) {
		client.doStart(c -> {
			try {
				URI uri = currentURI;
				NettyHttpChannel ch = (NettyHttpChannel) c;
				ch.getNettyRequest()
				  .setUri(uri.getPath() + (uri.getQuery() == null ? "" : "?" + uri.getQuery()))
				  .setMethod(new HttpMethod(method.name()))
				  .headers()
				  .add(HttpHeaderNames.HOST, uri.getHost())
				  .add(HttpHeaderNames.ACCEPT, "*/*");

				ch.delegate()
				  .pipeline()
				  .fireUserEventTriggered(new NettyHttpClientHandler.ChannelInputSubscriberEvent(subscriber));

				if (handler != null) {
					return handler.apply(ch);
				}
				else {
					HttpUtil.setTransferEncodingChunked(ch.getNettyResponse(), false);
					return ch.writeHeaders();
				}
			}
			catch (Throwable t) {
				return Mono.error(t);
			}
		})
		      .subscribe(Subscribers.unbounded(null,
				      (Consumer<Throwable>) reason -> EmptySubscription.error(subscriber, reason)));
	}
}
