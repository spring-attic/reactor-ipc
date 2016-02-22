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

import org.mockito.Mockito;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * @author Anatoly Kadyshev
 */
public class TestAeronInfra implements AeronInfra {

	private volatile boolean shouldFailClaim = false;

	@Override
	public void initialise() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public Publication addPublication(String channel, int streamId) {
		return Mockito.mock(Publication.class);
	}

	@Override
	public Subscription addSubscription(String channel, int streamId) {
		return Mockito.mock(Subscription.class);
	}

	@Override
	public long claim(Publication publication, BufferClaim bufferClaim, int length, IdleStrategy idleStrategy,
					  boolean retryClaim) {
		if (shouldFailClaim) {
			return -1;
		} else {
			bufferClaim.wrap(new UnsafeBuffer(new byte[length + 128]), 0, length + 128);
			return 1;
		}
	}

	@Override
	public void close(Publication publication) {
	}

	@Override
	public void close(Subscription subscription) {
	}

	public void setShouldFailClaim(boolean shouldFailClaim) {
		this.shouldFailClaim = shouldFailClaim;
	}

}
