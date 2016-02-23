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
package reactor.aeron.utils;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * @author Anatoly Kadyshev
 */
public interface AeronInfra {

	void initialise();

	void shutdown();

	Publication addPublication(String channel, int streamId);

	Subscription addSubscription(String channel, int streamId);

	long claim(Publication publication, BufferClaim bufferClaim, int length, IdleStrategy idleStrategy,
			   boolean retryClaim);

	void close(Publication publication);

	void close(Subscription subscription);

}
