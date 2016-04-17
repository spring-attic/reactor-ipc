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
package reactor.io.netty.common;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.Channel;
import reactor.io.ipc.Inbound;

/**
 * @author Stephane Maldini
 */
public interface NettyInbound extends Inbound<Buffer> {

	@Override
	io.netty.channel.Channel delegate();

	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return Lifecycle to build the events handlers
	 */
	NettyChannel.Lifecycle on();

	/**
	 * Get the inbound publisher (incoming tcp traffic for instance) and decode its traffic
	 *
	 * @param codec a decoding {@link NettyCodec} providing a target type {@link Publisher}
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	default <NEW_IN> Flux<NEW_IN> receive(NettyCodec<NEW_IN, ?> codec) {
		return receive(codec.decoder());
	}

	/**
	 * a {@link ByteBuffer} inbound {@link Flux}
	 *
	 * @return a {@link ByteBuffer} inbound {@link Flux}
	 */
	default Flux<ByteBuffer> receiveByteBuffer() {
		return receive().map(Buffer::byteBuffer);
	}

	/**
	 * a {@literal byte[]} inbound {@link Flux}
	 *
	 * @return a {@literal byte[]} inbound {@link Flux}
	 */
	default Flux<byte[]> receiveByteArray() {
		return receive().map(Buffer::asBytes);
	}

	/**
	 * a {@link String} inbound {@link Flux}
	 *
	 * @return a {@link String} inbound {@link Flux}
	 */
	default Flux<String> receiveString() {
		return receive().map(Buffer::asString);
	}

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();
}
