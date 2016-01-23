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
import reactor.aeron.support.DemandTracker;
import reactor.core.trait.Introspectable;
import reactor.core.trait.Subscribable;
import uk.co.real_logic.aeron.Publication;

/**
 * @author Anatoly Kadyshev
 */
class UnicastSession implements Session, Introspectable, Subscribable {

	private static int nextSessionUid = 0;

	private final Publication publication;

	private final Publication errorPublication;

	private volatile long lastHeartbeatTimeNs;

	private final DemandTracker demandTracker = new DemandTracker();

	private final String sessionId;

	public volatile Subscription subscription;

	private volatile boolean isAsyncSenderModeOn;

	private final int sessionUid;

	private volatile boolean terminal;

	public UnicastSession(String sessionId, Publication publication, Publication errorPublication) {
		this.sessionUid = nextSessionUid++;
		this.sessionId = sessionId;
		this.publication = publication;
		this.errorPublication = errorPublication;
	}

	public Publication getPublication() {
		return publication;
	}

	@Override
	public String toString() {
		return this.getClass().getName() +	"["
				+ "sessionId=" + sessionId
				+ ", lastHeartbeatTimeNs=" + lastHeartbeatTimeNs
				+ "]";
	}

	@Override
	public int getMode() {
		return INNER;
	}

	@Override
	public String getName() {
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

	public Publication getErrorPublication() {
		return errorPublication;
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
		this.terminal = true;
	}

	@Override
	public boolean isTerminal() {
		return terminal;
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	public void setAsyncSenderModeOn(boolean isAsyncSenderModeOn) {
		this.isAsyncSenderModeOn = isAsyncSenderModeOn;
	}

	public boolean isAsyncSenderModeOn() {
		return isAsyncSenderModeOn;
	}

	public int getSessionUid() {
		return sessionUid;
	}

	public long getAndResetDemad() {
		return demandTracker.getAndReset();
	}

	@Override
	public Object downstream() {
		return publication.channel()+"/"+publication.streamId();
	}
}
