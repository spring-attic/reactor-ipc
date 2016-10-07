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
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.publisher.Mono;
import reactor.ipc.Inbound;
import reactor.ipc.Outbound;
import reactor.ipc.connector.Connector;
import reactor.ipc.connector.StreamEndpoint;
import reactor.ipc.connector.StreamRemote;

/**
 * @author Stephane Maldini
 */
abstract class SimplePeer
		implements Connector<byte[], byte[]>, BiConsumer<Inbound<byte[]>, StreamEndpoint>,
		           Function<Outbound<byte[]>, StreamRemote> {

	@Override
	@SuppressWarnings("unchecked")
	public void accept(Inbound<byte[]> inbound, StreamEndpoint endpoint) {
		inbound.receive()
		       .subscribe(d -> ByteArrayStreamProtocol.receive(((SimpleConnection) inbound).in,
				       d,
				       endpoint));
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamRemote apply(Outbound<byte[]> outbound) {
		return (StreamRemote) outbound;
	}

	@Override
	public <API> Mono<API> newBidirectional(Supplier<?> receiverSupplier,
			Class<? extends API> api) {
		return newStreamSupport(receiverSupplier, api, this, this);
	}
}
