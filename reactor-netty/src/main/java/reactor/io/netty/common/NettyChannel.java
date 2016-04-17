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

/**
 * @author Stephane Maldini
 */
public interface NettyChannel extends Channel<Buffer, Buffer>, NettyInbound, NettyOutbound {

	@Override
	io.netty.channel.Channel delegate();

	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return Lifecycle to build the events handlers
	 */
	Lifecycle on();

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();

	/**
	 * Lifecycle Builder for assigning multiple event handlers on a channel.
	 */
	interface Lifecycle {

		/**
		 * Assign a {@link Runnable} to be invoked when the channel is closed.
		 *
		 * @param onClose the close event handler
		 * @return {@literal this}
		 */
		Lifecycle close(Runnable onClose);

		/**
		 * Assign a {@link Runnable} to be invoked when reads have become idle for the given timeout.
		 *
		 * @param idleTimeout the idle timeout
		 * @param onReadIdle  the idle timeout handler
		 * @return {@literal this}
		 */
		Lifecycle readIdle(long idleTimeout, Runnable onReadIdle);

		/**
		 * Assign a {@link Runnable} to be invoked when writes have become idle for the given timeout.
		 *
		 * @param idleTimeout the idle timeout
		 * @param onWriteIdle the idle timeout handler
		 * @return {@literal this}
		 */
		Lifecycle writeIdle(long idleTimeout, Runnable onWriteIdle);
	}
}
