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
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

/**
 * A {@link Outbound} is a reactive gateway for outgoing data flows. Like a Flux or a Mono
 * it can be chained via {@link #send} and a subscribe to the tailing outbound will
 * schedule all the parent send in the declaration order.
 * <p>
 * Writing and "flushing" is controlled by sinking 1 or more {@link #send(Publisher)} that
 * will forward data to outbound. When a drained Publisher completes or error, the channel
 * will automatically "flush" its pending writes.
 *
 * @author Stephane Maldini
 * @since 0.6
 */
@FunctionalInterface
public interface Outbound<OUT> extends Publisher<Void> {

	/**
	 * Return a never completing {@link Mono} after this {@link Outbound#then()} has
	 * completed.
	 *
	 * @return a never completing {@link Mono} after this {@link Outbound#then()} has
	 * completed.
	 */
	default Mono<Void> neverComplete() {
		return then(Mono.never()).then();
	}

	/**
	 * Send data to the peer, listen for any error on write and close on terminal signal
	 * (complete|error). <p>A new {@link Outbound} type (or the same) for typed send
	 * sequences. An implementor can therefore specialize the Outbound after a first after
	 * a prepending data publisher.
	 *
	 * @param dataStream the dataStream publishing OUT items to write on this channel
	 *
	 * @return A new {@link Outbound} to append further send. It will emit a complete
	 * signal successful sequence write (e.g. after "flush") or  any error during write.
	 */
	Outbound<OUT> send(Publisher<? extends OUT> dataStream);

	/**
	 * Subscribe a {@code Void} subscriber to this outbound and trigger all eventual
	 * parent outbound send.
	 *
	 * @param s the {@link Subscriber} to listen for send sequence completion/failure
	 */
	@Override
	default void subscribe(Subscriber<? super Void> s) {
		then().subscribe(s);
	}

	/**
	 * Obtain a {@link Mono} of pending outbound(s) write completion.
	 *
	 * @return a {@link Mono} of pending outbound(s) write completion
	 */
	default Mono<Void> then() {
		return Mono.empty();
	}

	/**
	 * Append a {@link Publisher} task such as a Mono and return a new
	 * {@link Outbound} to sequence further send.
	 *
	 * @param other the {@link Publisher} to subscribe to when this pending outbound
	 * {@link #then} is complete;
	 *
	 * @return a new {@link Outbound}
	 */
	default Outbound<OUT> then(Publisher<Void> other) {
		return new OutboundThen<>(this, other);
	}
}
