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

package reactor.ipc;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.Cancellation;
import reactor.core.publisher.Mono;

/**
 * An IPC binder is a channel factory sharing configuration but usually no runtime
 * (connection...) state at the exception of shared connection pool setups. Subscribing
 * to the returned {@link Mono} will effectively
 * create a new stateful "client" or "server" socket depending on the implementation.
 * It might also be working on top of a socket pool or connection pool as well, but the
 * state should be safely handled by the pool itself.
 * <p>
 * <p>Clients or Receivers will onSubscribe when their connection is established. They
 * will complete when the unique returned closing {@link Publisher} completes itself or if
 * the connection is remotely terminated. Calling the returned {@link
 * Cancellation#dispose()} from {@link Mono#subscribe()} will terminate the subscription
 * and underlying connection from the local peer.
 * <p>
 * <p>Servers or Producers will onSubscribe when their socket is bound locally. They will
 * never complete as many {@link Publisher} close selectors will be expected. Disposing
 * the returned {@link Mono} will safely call shutdown.
 */
public interface IpcBinder<IN, OUT> {

	/**
	 * Prepare a {@link Function} {@link Channel} handler that will bind each time the
	 * returned  {@link Mono} is subscribed. This {@link IpcBinder} shouldn't assume
	 * any state related to the individual created/cleaned resources.
	 * <p>
	 * The channel handler will return {@link Publisher} to signal when to terminate
	 * the underlying resource channel.
	 *
	 * @param channelHandler
	 *
	 * @return a {@link Mono} completing when the underlying resource has been closed or
	 * failed
	 */
	Mono<Void> bind(Function<? super Channel<IN, OUT>, ? extends Publisher<Void>> channelHandler);

}
