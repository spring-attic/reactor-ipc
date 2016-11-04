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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static reactor.core.publisher.Flux.just;

/**
 * A {@link Outbound} is a reactive gateway for outgoing data flows.
 * <p>
 * Writing and "flushing" is controlled by sinking 1 or more {@link #send(Publisher)}
 * that will forward data to outbound.
 * When a drained Publisher completes or error, the channel will automatically "flush" its pending writes.
 *
 * @author Stephane Maldini
 * @since 0.6
 */
@FunctionalInterface
public interface Outbound<OUT>  {

	/**
	 * Send data to the peer, listen for any error on write and close on terminal signal (complete|error).
	 *
	 * @param dataStream the dataStream publishing OUT items to write on this channel
	 * @return A {@link Mono} to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	Mono<Void> send(Publisher<? extends OUT> dataStream);

}
