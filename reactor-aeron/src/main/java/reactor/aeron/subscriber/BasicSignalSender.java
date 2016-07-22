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
package reactor.aeron.subscriber;

import java.util.function.Consumer;

import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.SignalPublicationFailedException;
import reactor.aeron.utils.SignalType;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * Publishes signals into Aeron
 *
 * Not thread-safe
 *
 * @author Anatoly Kadyshev
 */
public final class BasicSignalSender implements SignalSender {

	private final AeronInfra aeronInfra;

	private final Consumer<Throwable> errorConsumer;

	private final BufferClaim bufferClaim = new BufferClaim();

	private final IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

	public BasicSignalSender(AeronInfra aeronInfra, Consumer<Throwable> errorConsumer) {
		this.aeronInfra = aeronInfra;
		this.errorConsumer = errorConsumer;
	}

	/**
	 * Publishes a signals into Aeron and reports an exception into {@link #errorConsumer} when publication fails
	 *
	 * @param buffer to be published
	 * @param signalType type of signal
	 * @return true if successfully published signal and false otherwise
	 */
	@Override
	public long publishSignal(String sessionId, Publication publication, Buffer buffer, SignalType signalType,
							  boolean retryPublication) {
		long result;
		Throwable cause = null;
		try {
			result = doPublishSignal(publication, buffer, signalType, retryPublication);
		} catch (Throwable t) {
			result = Integer.MIN_VALUE;
			cause = t;
		}
		if (result < 0) {
			errorConsumer.accept(
					new SignalPublicationFailedException(sessionId, buffer, signalType, cause));
		}
		return result;
	}

	/**
	 * Publishes a signal into Aeron publication and either returns fails or throws an exception when publication fails
	 *
	 * @param publication
	 * @param buffer
	 * @param signalType
	 *
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 *
	 * @return true if signal was published and false otherwise
	 */
	private long doPublishSignal(Publication publication, Buffer buffer, SignalType signalType, boolean retryPublication) {
		long result = aeronInfra.claim(publication, bufferClaim, buffer.limit() + 1, idleStrategy, retryPublication);
		if (result >= 0) {
			try {
				MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
				int offset = bufferClaim.offset();
				mutableBuffer.putByte(offset, signalType.getCode());
				mutableBuffer.putBytes(offset + 1, buffer.byteBuffer().array(), buffer.position(), buffer.limit());
			} finally {
				bufferClaim.commit();
			}
		}
		return result;
	}
}
