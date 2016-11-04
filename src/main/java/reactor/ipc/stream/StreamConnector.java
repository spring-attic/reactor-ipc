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

package reactor.ipc.stream;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.Cancellation;
import reactor.core.publisher.Mono;
import reactor.ipc.connector.Connector;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;

/**
 * A stream connector is {@link Connector} supporting reactive streams semantics.
 *
 * @param <IN> the input connection data type (bytes, object...)
 * @param <OUT> the output connection data type (bytes, object...)
 * @param <INBOUND> incoming traffic API such as server request or client response
 * @param <OUTBOUND> outgoing traffic API such as server response or client request
 */
public interface StreamConnector<IN, OUT, INBOUND extends Inbound<IN>, OUTBOUND extends Outbound<OUT>>
		extends Connector<IN, OUT, INBOUND, OUTBOUND> {

	/**
	 *
	 * @param connector
	 * @param decoder
	 * @param encoder
	 * @param <IN>
	 * @param <OUT>
	 * @param <INBOUND>
	 * @param <OUTBOUND>
	 * @return a {@link StreamConnector}
	 */
	static <IN, OUT, INBOUND extends Inbound<IN>, OUTBOUND extends Outbound<OUT>> StreamConnector<IN, OUT, INBOUND, OUTBOUND> from(
			Connector<IN, OUT, INBOUND, OUTBOUND> connector,
			BiConsumer<? super INBOUND, StreamOperations> decoder,
			Function<? super OUTBOUND, ? extends StreamOutbound> encoder) {
		return new SimpleStreamConnector<>(connector, decoder, encoder);
	}

	/**
	 * @param receiverSupplier
	 *
	 * @return
	 */
	default Mono<? extends Cancellation> newReceiver(Supplier<?> receiverSupplier) {
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
	<API> Mono<API> newBidirectional(Supplier<?> receiverSupplier,
			Class<? extends API> api);
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
			BiConsumer<? super INBOUND, StreamOperations> decoder,
			Function<? super OUTBOUND, ? extends StreamOutbound> encoder) {
		return StreamSetup.connect(this, receiverSupplier, api, decoder, encoder);
	}

}
