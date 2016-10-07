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
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A {@link Inbound} is a reactive gateway for incoming data flows.
 * Implementations handle interacting inbound (received data) and errors by subscribing to {@link #receive()}.
 * <p>
 *
 * @author Stephane Maldini
 * @since 0.5
 */
@FunctionalInterface
public interface Inbound<IN>  {

	/**
	 * @return The underlying IO runtime connection reference (Netty Channel for instance)
	 */
	default Object delegate() {
		return null;
	}

	/**
	 * Get the inbound publisher (incoming tcp traffic for instance)
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	Flux<IN> receive();

	/**
	 * Get the inbound publisher (incoming tcp traffic for instance) and decode its traffic
	 *
	 * @param decoder a decoding function providing a target type {@link Publisher}
	 *
	 * @return A {@link Flux} to signal reads and stop reading when un-requested.
	 */
	default <NEW_IN> Flux<NEW_IN> receive(Function<? super Flux<IN>, ? extends Publisher<NEW_IN>> decoder) {
		return Flux.from(receive().as(decoder));
	}

	/**
	 * Get the Inbound-scoped {@link Scheduler}. Default to {@link
	 * Schedulers#immediate()}
	 *
	 * @return the Connector-scoped {@link Scheduler}
	 */

	default Scheduler inboundScheduler() {
		return Schedulers.immediate();
	}
}
