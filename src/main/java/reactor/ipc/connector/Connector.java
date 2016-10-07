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

package reactor.ipc.connector;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.Cancellation;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.Inbound;
import reactor.ipc.Outbound;

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
@FunctionalInterface
public interface Connector<IN, OUT> {

	/**
	 * @param receiverSupplier
	 *
	 * @return
	 */
	default Mono<Void> newReceiver(Supplier<?> receiverSupplier) {
		Objects.requireNonNull(receiverSupplier, "receiver");
		return newBidirectional(receiverSupplier, null);
	}

	/**
	 * @param api
	 * @param <API>
	 *
	 * @return
	 */
	default <API> Mono<API> newProducer(Class<? extends API> api) {
		Objects.requireNonNull(api, "api");
		return newBidirectional(null, api);
	}

	/**
	 * @param receiverSupplier
	 * @param api
	 * @param <API>
	 *
	 * @return
	 */
	default <API> Mono<API> newBidirectional(Supplier<?> receiverSupplier,
			Class<? extends API> api) {
		throw new UnsupportedOperationException("This Connector " + getClass().getSimpleName() + " does not support reactive stream protocol.");
	}

	/**
	 * Prepare a {@link BiFunction} IO handler that will react on a new connected state
	 * each
	 * time
	 * the returned  {@link Mono} is subscribed. This {@link Connector} shouldn't assume
	 * any state related to the individual created/cleaned resources.
	 * <p>
	 * The IO handler will return {@link Publisher} to signal when to terminate the
	 * underlying resource channel.
	 *
	 * @param channelHandler
	 *
	 * @return a {@link Mono} completing when the underlying resource has been closed or
	 * failed
	 */
	Mono<Void> newHandler(BiFunction<? super Inbound<IN>, ? super Outbound<OUT>, ? extends Publisher<Void>> channelHandler);

	/**
	 * @param receiverSupplier
	 * @param api
	 * @param decoder
	 * @param encoder
	 * @param <API>
	 *
	 * @return
	 */
	default <API> Mono<API> newStreamSupport(Supplier<?> receiverSupplier,
			Class<? extends API> api,
			BiConsumer<? super Inbound<IN>, StreamEndpoint> decoder,
			Function<? super Outbound<OUT>, ? extends StreamRemote> encoder) {
		return ConnectorHelper.connect(this, receiverSupplier, api, decoder, encoder);
	}

	/**
	 * Get the Connector-scoped {@link Scheduler}. Default to {@link
	 * Schedulers#immediate()}
	 *
	 * @return the Connector-scoped {@link Scheduler}
	 */

	default Scheduler scheduler() {
		return Schedulers.immediate();
	}
}
