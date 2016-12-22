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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 * An appending write that delegates to its origin context and append the passed
 * publisher after the origin success if any.
 *
 * @param <OUT> sending type
 */
final class OutboundThen<OUT> implements Outbound<OUT> {

	final Mono<Void> thenMono;
	final Outbound<OUT> source;

	OutboundThen(Outbound<OUT> source, Publisher<Void> thenPublisher) {
		Mono<Void> parentMono = source.then();
		this.source = source;
		if (parentMono == Mono.<Void>empty()) {
			this.thenMono = Mono.from(thenPublisher);
		}
		else {
			this.thenMono = parentMono.thenEmpty(thenPublisher);
		}
	}

	@Override
	public Outbound<OUT> send(Publisher<? extends OUT> dataStream) {
		return source.send(dataStream);
	}

	@Override
	public Mono<Void> then() {
		return thenMono;
	}
}
