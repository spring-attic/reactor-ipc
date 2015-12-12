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
package reactor.aeron.publisher;

import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.aeron.support.ServiceMessageType;
import reactor.core.support.ReactiveState;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;


/**
 * @author Anatoly Kadyshev
 */
public class ServiceMessageSender implements ReactiveState.Upstream, ReactiveState.FeedbackLoop {

	private final AeronInfra aeronInfra;

	private final Publication serviceRequestPub;

	private final BufferClaim bufferClaim = new BufferClaim();

	private final BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

	private final byte[] sessionId;

	public ServiceMessageSender(AeronInfra aeronInfra, Publication serviceRequestPub, String sessionId) {
		this.aeronInfra = aeronInfra;
		this.serviceRequestPub = serviceRequestPub;
		this.sessionId = sessionId.getBytes(AeronUtils.UTF_8_CHARSET);
	}

	/**
	 * Sends service request message for n signals
	 *
	 * @param n number of signals to request
	 *
	 * @throws RuntimeException when Aeron publication was either backpressured or not connected
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 */
	public synchronized void sendRequest(long n) {
		if (claimBuffer(1 + 8 + (sessionId.length + 1)) >= 0) {
			commitRequest(bufferClaim, n, sessionId);
		} else {
			//TODO: Handle a situation when service request cannot be sent
			throw new RuntimeException("Failed to send request service message" +
					" due to backpressured/not connected publication");
		}
	}

	/**
	 * Sends service cancel message
	 *
	 * @throws RuntimeException when Aeron publication was either backpressured or not connected
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 */
	public synchronized void sendCancel() {
		long position = claimBuffer(1 + (sessionId.length + 1));
		if (position >= 0) {
			commitCancel(bufferClaim, sessionId);
		} else {
			throw new RuntimeException("Failed to send cancel service message" +
					" due to backpressured/not connected publication");
		}
	}

	/**
	 * Sends service heartbeat message
	 *
	 * @throws RuntimeException when Aeron publication was either backpressured or not connected
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 */
	public synchronized long sendHeartbeat() {
		long result = claimBuffer(1 + (sessionId.length + 1));
		if (result >= 0) {
			commitHeartbeat(bufferClaim, sessionId);
		}
		return result;
	}

	/**
	 * Claims a buffer for publishing into Aeron
	 *
	 * @throws IllegalStateException
	 * @throws IllegalArgumentException if publication is closed
	 *
	 * @return buffer claim when successful and null when Aeron publication was either backpressured or not connected
	 */
	private long claimBuffer(int length) {
		return aeronInfra.claim(serviceRequestPub, bufferClaim, length,	idleStrategy, true);
	}

	private void putSessionId(MutableDirectBuffer mutableBuffer, int offset, byte[] sessionId) {
		mutableBuffer.putByte(offset, (byte) sessionId.length);
		mutableBuffer.putBytes(offset + 1, sessionId);
	}

	private void commitRequest(BufferClaim bufferClaim, long n, byte[] sessionId) {
		try {
			MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
			int offset = bufferClaim.offset();
			mutableBuffer.putByte(offset, ServiceMessageType.Request.getCode());
			mutableBuffer.putLong(offset + 1, n);
			putSessionId(mutableBuffer, offset + 1 + 8, sessionId);
		} finally {
			bufferClaim.commit();
		}
	}

	private void commitCancel(BufferClaim bufferClaim, byte[] sessionId) {
		try {
			MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
			int offset = bufferClaim.offset();
			mutableBuffer.putByte(offset, ServiceMessageType.Cancel.getCode());
			putSessionId(mutableBuffer, offset + 1, sessionId);
		} finally {
			bufferClaim.commit();
		}
	}

	private void commitHeartbeat(BufferClaim bufferClaim, byte[] sessionId) {
		try {
			MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
			int offset = bufferClaim.offset();
			mutableBuffer.putByte(offset, ServiceMessageType.Heartbeat.getCode());
			putSessionId(mutableBuffer, offset + 1, sessionId);
		} finally {
			bufferClaim.commit();
		}
	}

	@Override
	public Object upstream() {
		return serviceRequestPub.channel();
	}

	@Override
	public Object delegateInput() {
		return aeronInfra;
	}

	@Override
	public Object delegateOutput() {
		return aeronInfra;
	}
}
