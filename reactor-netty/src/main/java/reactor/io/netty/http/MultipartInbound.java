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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.io.netty.common.NettyInbound;

/**
 * An Http Reactive Multipart read contract for incoming traffic.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public interface MultipartInbound extends NettyInbound {

	/**
	 * a {@link ByteBuf} inbound parts as {@link Flux}
	 *
	 * @return a {@link ByteBuf} partitioned inbound {@link Flux}
	 */
	Flux<Flux<ByteBuf>> receiveParts();

	@Override
	default Flux<ByteBuf> receive() {
		return receiveParts().onBackpressureBuffer()
		                     .concatMap(parts -> parts.toList()
		                                            .map(list -> Unpooled.wrappedBuffer(
				                                            list.toArray(new
						                                            ByteBuf[list.size()
						                                            ]))), Integer.MAX_VALUE);
	}

	@Override
	default Flux<?> receiveObject() {
		return receive();
	}
}
