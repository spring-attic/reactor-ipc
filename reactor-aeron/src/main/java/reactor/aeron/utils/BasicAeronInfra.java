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

import java.util.concurrent.TimeUnit;

import reactor.aeron.Context;
import reactor.util.Logger;

import reactor.util.Loggers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * Helper class for creating Aeron subscriptions and publications and
 * publishing messages.
 *
 * @author Anatoly Kadyshev
 */
public final class BasicAeronInfra implements AeronInfra {

	private static final Logger logger = Loggers.getLogger(BasicAeronInfra.class);

	private final boolean launchEmbeddedMediaDriver;

	private volatile Aeron aeron;

	/**
	 * @see Context#publicationRetryMillis
	 */
	private final long publicationRetryNs;

	public BasicAeronInfra(Aeron aeron, long publicationRetryMillis) {
		this.launchEmbeddedMediaDriver = (aeron == null);
		this.aeron = aeron;
		this.publicationRetryNs = TimeUnit.MILLISECONDS.toNanos(publicationRetryMillis);
	}

	@Override
	public void initialise() {
		if (launchEmbeddedMediaDriver && aeron == null) {
			EmbeddedMediaDriverManager driverManager = EmbeddedMediaDriverManager.getInstance();
			driverManager.launchDriver();
			this.aeron = driverManager.getAeron();
		}
	}

	@Override
	public void shutdown() {
		if (launchEmbeddedMediaDriver && aeron != null) {
			EmbeddedMediaDriverManager.getInstance().shutdownDriver();
			this.aeron = null;
		}
	}

	@Override
	public Publication addPublication(String channel, int streamId) {
		if (logger.isDebugEnabled()) {
			logger.debug("Created publication for channel: {}, streamId: {}", channel, streamId);
		}
		return aeron.addPublication(channel, streamId);
	}

	@Override
	public Subscription addSubscription(String channel, int streamId) {
		if (logger.isDebugEnabled()) {
			logger.debug("Created subscription for channel: {}, streamId: {}", channel, streamId);
		}
		return aeron.addSubscription(channel, streamId);
	}

	/**
	 * Reserves a buffer claim to be used for publishing into Aeron
	 *
	 * @param publication  into which data should be published
	 * @param bufferClaim  to be used for publishing
	 * @param length        number of bytes to be published
	 * @param idleStrategy idle strategy to use when an attempt
	 *                     to claim a buffer for publishing fails
	 *
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 *
	 * @return @see Publication#tryClaim(int, BufferClaim)
	 *
	 */
	@Override
	public long claim(Publication publication, BufferClaim bufferClaim, int length,
					  IdleStrategy idleStrategy, boolean retryClaim) {
		long startTime = System.nanoTime();
		long result;
		while ((result = publication.tryClaim(length, bufferClaim)) < 0) {
			if (result == Publication.CLOSED) {
				break;
			}

			if (!retryClaim || System.nanoTime() - startTime > publicationRetryNs) {
				break;
			}
			idleStrategy.idle(0);
		}
		idleStrategy.idle(1);
		return result;
	}

	@Override
	public void close(Publication publication) {
		publication.close();

		if (logger.isDebugEnabled()) {
			logger.debug("Closed publication for channel: {}, streamId: {}",
					publication.channel(), publication.streamId());
		}
	}

	@Override
	public void close(Subscription subscription) {
		subscription.close();

		if (logger.isDebugEnabled()) {
			logger.debug("Closed subscription for channel: {}, streamId: {}",
					subscription.channel(), subscription.streamId());
		}
	}

}
