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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.io.buffer.Buffer;
import reactor.io.netty.common.NettyCodec;

/**
 * An Http Reactive client read contract for incoming response. It inherits several accessor related to HTTP
 * flow : headers, params,
 * URI, method, websocket...
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface HttpInbound extends HttpConnection {

	/**
	 * Get the inbound http traffic
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	Flux<Buffer> receiveBody();

	/**
	 * Get the inbound http traffic and decode it
	 *
	 * @param decoder a decoding function providing a target type {@link Publisher}
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	default <NEW_IN> Flux<NEW_IN> receiveBody(Function<? super Flux<Buffer>, ? extends Publisher<NEW_IN>> decoder) {
		return Flux.from(receiveBody().as(decoder));
	}

	/**
	 * Get the inbound http traffic and decode it
	 *
	 * @param codec a decoding {@link NettyCodec} providing a target type {@link Publisher}
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	default <NEW_IN> Flux<NEW_IN> receiveBody(NettyCodec<NEW_IN, ?> codec) {
		return receiveBody(codec.decoder());
	}

	/**
	 * a {@link ByteBuffer} inbound {@link Flux}
	 *
	 * @return a {@link ByteBuffer} inbound {@link Flux}
	 */
	default Flux<ByteBuffer> receiveByteBufferBody() {
		return receiveBody().map(Buffer::byteBuffer);
	}

	/**
	 * a {@literal byte[]} inbound {@link Flux}
	 *
	 * @return a {@literal byte[]} inbound {@link Flux}
	 */
	default Flux<byte[]> receiveByteArrayBody() {
		return receiveBody().map(Buffer::asBytes);
	}

	/**
	 * a {@link String} inbound {@link Flux}
	 *
	 * @return a {@link String} inbound {@link Flux}
	 */
	default Flux<String> receiveStringBody() {
		return receiveBody().map(Buffer::asString);
	}

	/**
	 * @return the resolved HTTP Response Status
	 */
	HttpResponseStatus status();


}
