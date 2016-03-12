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

package reactor.io.netty;

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.timer.Timer;
import reactor.core.tuple.Tuple2;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;

/**
 * A network-aware client that will publish its connection once available to the {@link
 * ChannelFluxHandler} passed.
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 * @param <CONN> the channel implementation
 * @author Stephane Maldini
 */
public abstract class ReactiveClient<IN, OUT, CONN extends ChannelFlux<IN, OUT>>
		extends ReactivePeer<IN, OUT, CONN> {

	public static final ChannelFluxHandler PING = o -> Flux.empty();

	protected ReactiveClient(Timer defaultEnv, long prefetch) {
		super(defaultEnv, prefetch);
	}

	/**
	 * Open a channel to the configured address and return a {@link Publisher} that will
	 * be populated by the {@link ChannelFlux} every time a connection or reconnection
	 * is made. <p> The returned {@link Publisher} will typically complete when all
	 * reconnect options have been used, or error if anything wrong happened during the
	 * (re)connection process.
	 * @param reconnect the reconnection strategy to use when disconnects happen
	 * @return a Publisher of reconnected address and accumulated number of attempt pairs
	 */
	public final Flux<Tuple2<InetSocketAddress, Integer>> start(
			ChannelFluxHandler<IN, OUT, CONN> handler, Reconnect reconnect) {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Client already started");
		}

		return doStart(handler, reconnect);
	}

	protected abstract Flux<Tuple2<InetSocketAddress, Integer>> doStart(
			ChannelFluxHandler<IN, OUT, CONN> handler, Reconnect reconnect);


}