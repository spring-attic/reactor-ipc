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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.tuple.Tuple2;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.Reconnect;
import reactor.io.net.tcp.TcpClient;
import reactor.rx.Fluxion;
import reactor.rx.net.ChannelFluxion;
import reactor.rx.net.ReactorChannelHandler;
import reactor.rx.net.ReactorPeer;

/**
 * A network-aware client that will publish its connection once available to the {@link
 * ReactiveChannelHandler} passed.
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 * @author Stephane Maldini
 */
public final class ReactorTcpClient<IN, OUT> extends ReactorPeer<IN, OUT, TcpClient<IN, OUT>> {

	/**
	 *
	 * @param peer
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactorTcpClient<IN, OUT> create(TcpClient<IN, OUT> peer) {
		return new ReactorTcpClient<>(peer);
	}

	protected ReactorTcpClient(TcpClient<IN, OUT> client) {
		super(client);
	}

	/**
	 * Start this {@literal ReactorPeer}.
	 * @return a {@link Mono<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public Mono<Void> start(ReactiveChannelHandler<IN, OUT, ChannelFluxion<IN, OUT>> handler) {
		return peer.start(
				ChannelFluxion.wrap(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
		);
	}

	/**
	 *
	 * @return
	 */
	public InetSocketAddress getConnectAddress() {
		return peer.getConnectAddress();
	}


	/**
	 * Open a channel to the configured address and return a {@link Publisher} that will
	 * be populated by the {@link ReactiveChannel} every time a connection or reconnection
	 * is made. <p> The returned {@link Publisher} will typically complete when all
	 * reconnect options have been used, or error if anything wrong happened during the
	 * (re)connection process.
	 * @param reconnect the reconnection strategy to use when disconnects happen
	 * @return a Publisher of reconnected address and accumulated number of attempt pairs
	 */
	public Fluxion<Tuple2<InetSocketAddress, Integer>> start(
			ReactorChannelHandler<IN, OUT> handler, Reconnect reconnect) {
		return Fluxion.from(
				peer.start(
				ChannelFluxion.wrap(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize())
				, reconnect)
		);
	}

}
