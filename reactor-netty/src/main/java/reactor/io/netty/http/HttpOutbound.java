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

import java.nio.ByteBuffer;
import java.util.function.Function;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.io.buffer.Buffer;
import reactor.io.netty.common.NettyCodec;

/**
 * An Http Reactive client write contract for outgoing requests. It inherits several accessor related to HTTP flow :
 * headers, params, URI, method, websocket...
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface HttpOutbound extends HttpConnection {

	/**
	 * add the passed cookie
	 *
	 * @return this
	 */
	HttpOutbound addCookie(Cookie cookie);

	/**
	 * @param name
	 * @param value
	 *
	 * @return
	 */
	HttpOutbound addHeader(CharSequence name, CharSequence value);

	/**
	 * @param name
	 * @param value
	 *
	 * @return
	 */
	HttpOutbound header(CharSequence name, CharSequence value);

	/**
	 * @return Resolved HTTP request headers
	 */
	HttpHeaders headers();

	/**
	 * set the request keepAlive if true otherwise remove the existing connection keep alive header
	 *
	 * @return is keep alive
	 */
	HttpOutbound keepAlive(boolean keepAlive);

	/**
	 *
	 */
	HttpOutbound removeTransferEncodingChunked();

	/**
	 * Send data to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error).
	 *
	 * @param dataStream the dataStream publishing OUT items to write on this channel
	 *
	 * @return A {@link Mono} to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	Mono<Void> sendBody(Publisher<? extends Buffer> dataStream);

	/**
	 * Send data to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error).
	 *
	 * @param dataStream the dataStream publishing OLD_OUT items to write on this channel after encoding
	 * @param codec an encoding {@link NettyCodec} providing a send-ready {@link Publisher}
	 *
	 * @return A {@link Mono} to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default <OLD_OUT> Mono<Void> sendBody(Publisher<? extends OLD_OUT> dataStream, NettyCodec<?, OLD_OUT> codec) {
		return sendBody(dataStream, codec.encoder());
	}

	/**
	 * Send data to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error).
	 *
	 * @param dataStream the dataStream publishing OLD_OUT items to write on this channel after encoding
	 * @param encoder an encoding function providing a send-ready {@link Publisher}
	 *
	 * @return A {@link Mono} to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default <OLD_OUT> Mono<Void> sendBody(Publisher<? extends OLD_OUT> dataStream,
			Function<Flux<? extends OLD_OUT>, ? extends Publisher<Buffer>> encoder) {

		return sendBody(Flux.from(dataStream)
		                    .as(encoder));
	}

	/**
	 * /** Send bytes to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error). If more than one publisher is attached (multiple calls to send()) completion occurs after all
	 * publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendByteArrayBody(Publisher<? extends byte[]> dataStream) {
		return sendBody(Flux.from(dataStream)
		                .map(Buffer::wrap));
	}

	/**
	 * Send bytes to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error). If more than one publisher is attached (multiple calls to send()) completion occurs after all
	 * publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendByteBufferBody(Publisher<? extends ByteBuffer> dataStream) {
		return sendBody(Flux.from(dataStream)
		                .map(Buffer::new));
	}

	/**
	 * Send String to the outbound http connection, listen for any error on write and close on terminal signal
	 * (complete|error). If more than one publisher is attached (multiple calls to send()) completion occurs after all
	 * publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendStringBody(Publisher<? extends String> dataStream) {
		return sendBody(Flux.from(dataStream)
		                    .map(Buffer::wrap));
	}

	/**
	 * @return
	 */
	Mono<Void> sendHeaders();
}
