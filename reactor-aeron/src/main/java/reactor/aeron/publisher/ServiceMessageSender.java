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

import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.ServiceMessageType;
import reactor.core.Loopback;
import reactor.core.Producer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;


/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
class ServiceMessageSender implements Producer, Loopback {

	private final Publication serviceRequestPub;

	private final BufferClaim bufferClaim = new BufferClaim();

	private final BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

	private final byte[] sessionIdEncoded;

	private final AeronFlux parent;

	private final String sessionId;

	public ServiceMessageSender(AeronFlux parent, Publication serviceRequestPub, String sessionId) {
		this.parent = parent;
		this.serviceRequestPub = serviceRequestPub;
		this.sessionId = sessionId;
		this.sessionIdEncoded = sessionId.getBytes(AeronUtils.UTF_8_CHARSET);
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
		if (claimBuffer(1 + 8 + (sessionIdEncoded.length + 1)) >= 0) {
			commitRequest(bufferClaim, n, sessionIdEncoded);
		} else {
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
		long position = claimBuffer(1 + (sessionIdEncoded.length + 1));
		if (position >= 0) {
			commitCancel(bufferClaim, sessionIdEncoded);
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
		long result = claimBuffer(1 + (sessionIdEncoded.length + 1));
		if (result >= 0) {
			commitHeartbeat(bufferClaim, sessionIdEncoded);
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
		return parent.aeronInfra.claim(serviceRequestPub, bufferClaim, length,	idleStrategy, true);
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
	public Object downstream() {
		return serviceRequestPub.channel()+"/"+serviceRequestPub.streamId();
	}

	@Override
	public Object connectedOutput() {
		return parent;
	}

	public String getSessionId() {
		return sessionId;
	}

}
