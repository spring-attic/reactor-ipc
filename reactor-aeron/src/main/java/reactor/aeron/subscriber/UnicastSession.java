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

import org.reactivestreams.Subscription;
import reactor.aeron.utils.DemandTracker;
import reactor.core.Producer;
import uk.co.real_logic.aeron.Publication;

/**
 * @author Anatoly Kadyshev
 */
class UnicastSession implements Session, Producer {

	private final Publication publication;

	private volatile long lastHeartbeatTimeNs;

	private final DemandTracker demandTracker = new DemandTracker();

	private final String sessionId;

	public volatile Subscription subscription;

	private volatile boolean isTerminal;

	public UnicastSession(String sessionId, Publication publication) {
		this.sessionId = sessionId;
		this.publication = publication;
	}

	public Publication getPublication() {
		return publication;
	}

	@Override
	public String toString() {
		return sessionId;
	}

	/**
	 * @param n number of signals to request
	 * @return the previous demand
	 */
	@Override
	public long requestMore(long n) {
		return demandTracker.request(n);
	}

	@Override
	public long getLastHeartbeatTimeNs() {
		return lastHeartbeatTimeNs;
	}

	@Override
	public void setLastHeartbeatTimeNs(long lastHeartbeatTimeNs) {
		this.lastHeartbeatTimeNs = lastHeartbeatTimeNs;
	}

	public void setTerminal() {
		this.isTerminal = true;
	}

	public boolean isTerminal() {
		return isTerminal;
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	public long getAndResetDemad() {
		return demandTracker.getAndReset();
	}

	@Override
	public Object downstream() {
		return publication.channel()+"/"+publication.streamId();
	}
}
