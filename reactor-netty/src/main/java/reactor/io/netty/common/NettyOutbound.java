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

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.FileRegion;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.io.ipc.Outbound;

/**
 * @author Stephane Maldini
 */
public interface NettyOutbound extends Outbound<ByteBuf> {

	@Override
	io.netty.channel.Channel delegate();

	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return Lifecycle to build the events handlers
	 */
	NettyChannel.Lifecycle on();

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();

	/**
	 * Send data to the peer, listen for any error on write and close on terminal signal (complete|error).
	 *
	 * @param dataStream the dataStream publishing OLD_OUT items to write on this channel after encoding
	 * @param codec an encoding {@link NettyCodec} providing a send-ready {@link Publisher}
	 *
	 * @return A {@link Mono} to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default <OLD_OUT> Mono<Void> send(Publisher<? extends OLD_OUT> dataStream, NettyCodec<?, OLD_OUT> codec) {
		return send(dataStream, codec.encoder());
	}

	/**
	 * /** Send bytes to the peer, listen for any error on write and close on terminal signal (complete|error). If more
	 * than one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendByteArray(Publisher<? extends byte[]> dataStream) {
		return send(Flux.from(dataStream)
		                .map(Unpooled::wrappedBuffer));
	}

	/**
	 * Send bytes to the peer, listen for any error on write and close on terminal signal (complete|error). If more than
	 * one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendByteBuffer(Publisher<? extends ByteBuffer> dataStream) {
		return send(Flux.from(dataStream)
		                .map(Unpooled::wrappedBuffer));
	}

	/**
	 * Send File with zero-byte copy to the peer, listen for any error on write and close on terminal signal
	 * (complete|error). If more
	 * than one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param file the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendFile(File file) {
		return sendFile(file, 0L, file.length());
	}

	/**
	 * Send File with zero-byte copy to the peer, listen for any error on write and close on terminal signal
	 * (complete|error). If more
	 * than one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param file the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendFile(File file, long position, long count) {
		return MonoChannelFuture.from(delegate().writeAndFlush(new DefaultFileRegion(file, position, count)));
	}

	/**
	 * Send String to the peer, listen for any error on write and close on terminal signal (complete|error). If more
	 * than one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendString(Publisher<? extends String> dataStream) {
		return sendString(dataStream, Charset.defaultCharset());
	}

	/**
	 * Send String to the peer, listen for any error on write and close on terminal signal (complete|error). If more
	 * than one publisher is attached (multiple calls to send()) completion occurs after all publishers complete.
	 *
	 * @param dataStream the dataStream publishing Buffer items to write on this channel
	 * @param charset the encoding charset
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	default Mono<Void> sendString(Publisher<? extends String> dataStream, Charset charset) {
		return send(Flux.from(dataStream)
		                .map(s -> delegate().alloc().buffer().writeBytes(s.getBytes(charset))));
	}
}
