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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSource;

/**
 * A decorating {@link Flux} {@link NettyInbound} with various {@link ByteBuf} related
 * operations.
 *
 * @author Stephane Maldini
 */
public final class EncodedFlux extends FluxSource<ByteBuf, ByteBuf> {

	/**
	 * Decorate as {@link EncodedFlux}
	 *
	 * @param source publisher to decorate
	 * @param allocator the channel {@link ByteBufAllocator}
	 *
	 * @return a {@link EncodedFlux}
	 */
	public final static EncodedFlux encoded(Publisher<? extends ByteBuf> source,
			ByteBufAllocator allocator) {
		return new EncodedFlux(source, allocator);
	}

	/**
	 * Disable auto memory release on each signal published in order to prevent premature
	 * recycling when buffers are accumulated downsteams (async).
	 *
	 * @return {@link EncodedMono} of retained {@link ByteBuf}
	 */
	public EncodedMono aggregate() {
		return this.reduceWith(alloc::compositeBuffer,
				(prev, next) -> prev.addComponent(next.retain()))
		           .doAfterNext(ByteBuf::release)
		           .where(ByteBuf::isReadable)
		           .doOnNext(cbb -> cbb.writerIndex(cbb.capacity()))
		           .as(EncodedMono::new);
	}

	/**
	 * Disable auto memory release on each signal published in order to prevent premature
	 * recycling when buffers are accumulated downsteams (async).
	 *
	 * @return {@link EncodedFlux} of retained {@link ByteBuf}
	 */
	public EncodedFlux retain() {
		return new EncodedFlux(doOnNext(ByteBuf::retain), alloc);
	}

	final ByteBufAllocator alloc;

	protected EncodedFlux(Publisher<? extends ByteBuf> source,
			ByteBufAllocator allocator) {
		super(source);
		this.alloc = allocator;
	}
}
