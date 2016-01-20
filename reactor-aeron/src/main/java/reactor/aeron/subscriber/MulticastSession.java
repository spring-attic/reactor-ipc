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

import reactor.core.queue.disruptor.Sequence;
import reactor.core.queue.disruptor.Sequencer;
import reactor.core.subscription.BackpressureUtils;

/**
 * @author Anatoly Kadyshev
 */
class MulticastSession implements Session {

	private final String sessionId;

	/**
	 * Sequence corresponding to demand = demand - 1
	 */
	private final Sequence sequence;

	private volatile long lastHeartbeatTimeNs;

	MulticastSession(String sessionId, long sequence) {
		this.sessionId = sessionId;
		this.sequence = Sequencer.newSequence(sequence);
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	public long requestMore(long n) {
		return BackpressureUtils.getAndAdd(sequence, n);
	}

	@Override
	public long getLastHeartbeatTimeNs() {
		return lastHeartbeatTimeNs;
	}

	@Override
	public void setLastHeartbeatTimeNs(long lastHeartbeatTimeNs) {
		this.lastHeartbeatTimeNs = lastHeartbeatTimeNs;
	}

	@Override
	public void setTerminal() {
	}

	@Override
	public boolean isTerminal() {
		return false;
	}

	public long getSequence() {
		return sequence.get();
	}

	@Override
	public String toString() {
		return "MulticastSession["
				+ "sequence=" + sequence
				+ "]";
	}
}
