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

package reactor.ipc.socket;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;
import reactor.ipc.stream.StreamConnector;
import reactor.ipc.stream.StreamOperations;
import reactor.ipc.stream.StreamOutbound;

/**
 * @author Stephane Maldini
 */
abstract class SimplePeer
		implements StreamConnector<byte[], byte[], Inbound<byte[]>, Outbound<byte[]>>,
		           BiConsumer<Inbound<byte[]>, StreamOperations>,
		           Function<Outbound<byte[]>, StreamOutbound> {

	@Override
	@SuppressWarnings("unchecked")
	public void accept(Inbound<byte[]> inbound, StreamOperations endpoint) {
		inbound.receive()
		       .subscribe(d -> ByteArrayStreamProtocol.receive(((SimpleConnection) inbound).in,
				       d,
				       endpoint));
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamOutbound apply(Outbound<byte[]> outbound) {
		return (StreamOutbound) outbound;
	}

	@Override
	public <API> Mono<API> newBidirectional(Supplier<?> receiverSupplier,
			Class<? extends API> api) {
		return newStreamSupport(receiverSupplier, api, this, this);
	}
}
