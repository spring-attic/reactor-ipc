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

package reactor.rx.net.tcp;

import java.net.InetSocketAddress;

import reactor.Mono;
import reactor.rx.net.ChannelStream;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.rx.net.ReactorPeer;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Promise;

/**
 * A network-aware client that will publish its connection once available to the {@link ReactiveChannelHandler} passed.
 *
 * @param <IN>   the type of the received data
 * @param <OUT>  the type of replied data
 * @author Stephane Maldini
 * @since 2.5
 */
public final class ReactorTcpServer<IN, OUT> extends ReactorPeer<IN, OUT, TcpServer<IN, OUT>> {

	/**
	 *
	 * @param peer
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactorTcpServer<IN, OUT> create(TcpServer<IN, OUT> peer){
		return new ReactorTcpServer<>(peer);
	}

	protected ReactorTcpServer(TcpServer<IN, OUT> peer) {
		super(peer);
	}


	/**
	 * Start this {@literal ReactorPeer}.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public Mono<Void> start(ReactiveChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler) {
		return peer.start(
				ChannelStream.wrap(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
		);
	}

	/**
	 * 
	 * @return
	 */
	public InetSocketAddress getListenAddress() {
		return peer.getListenAddress();
	}

}
