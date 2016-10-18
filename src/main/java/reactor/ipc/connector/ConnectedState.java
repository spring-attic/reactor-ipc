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

import java.util.function.BiFunction;

import reactor.core.Cancellation;
import reactor.core.publisher.Mono;

/**
 * A {@link Cancellation} token for {@link Connector} with additional lifecycle
 * operator and
 * accessors. Users will receive this token via
 * {@link Connector#newHandler(BiFunction)} on connected state, for instance when a
 * server is bound or a client is connected.
 *
 * @author Stephane Maldini
 */
public interface ConnectedState extends Cancellation {

	/**
	 * The underlying connected artifact (Socket, ServerSocket, Channel...)
	 *
	 * @return the underlying connected artifact (Socket, ServerSocket, Channel...)
	 */
	Object delegate();

	/**
	 * Return an observing {@link Mono} terminating with success when shutdown
	 * successfully
	 * or error.
	 *
	 * @return a {@link Mono} terminating with success if shutdown successfully or error
	 */
	Mono<Void> onClose();
}
