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
 * A {@link Channel} is a virtual connection that often matches with a Socket or a Channel (e.g. Netty).
 * Implementations handle interacting inbound (received data) and errors by subscribing to {@link #receive()}.
 * <p>
 * Writing and "flushing" is controlled by sinking 1 or more {@link #send(Publisher)}
 * that will forward data to outbound.
 * When a drained Publisher completes or error, the channel will automatically "flush" its pending writes.
 *
 * @author Stephane Maldini
 * @since 0.5
 */
public interface Channel<IN, OUT> extends Inbound<IN>, Outbound<OUT>  {

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
	 * @return The underlying IO runtime connection reference (Netty Channel for instance)
	 */
	@Override
	Object delegate();
}
