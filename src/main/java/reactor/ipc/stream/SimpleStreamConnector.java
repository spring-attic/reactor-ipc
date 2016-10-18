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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.ipc.connector.ConnectedState;
import reactor.ipc.connector.Connector;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;

/**
 * @author Stephane Maldini
 */
final class SimpleStreamConnector<IN, OUT, INBOUND extends Inbound<IN>, OUTBOUND extends Outbound<OUT>>
		implements StreamConnector<IN, OUT, INBOUND, OUTBOUND> {

	final Connector<IN, OUT, INBOUND, OUTBOUND>                connector;
	final BiConsumer<? super INBOUND, StreamOperations>        decoder;
	final Function<? super OUTBOUND, ? extends StreamOutbound> encoder;

	SimpleStreamConnector(Connector<IN, OUT, INBOUND, OUTBOUND> connector,
			BiConsumer<? super INBOUND, StreamOperations> decoder,
			Function<? super OUTBOUND, ? extends StreamOutbound> encoder) {
		this.connector = Objects.requireNonNull(connector, "connector");
		this.decoder = decoder;
		this.encoder = encoder;
	}

	@Override
	public <API> Mono<API> newBidirectional(Supplier<?> receiverSupplier,
			Class<? extends API> api) {
		return newStreamSupport(receiverSupplier, api, decoder, encoder);
	}

	@Override
	public Mono<? extends ConnectedState> newHandler(BiFunction<? super INBOUND, ? super OUTBOUND, ? extends Publisher<Void>> ioHandler) {
		return connector.newHandler(ioHandler);
	}
}
