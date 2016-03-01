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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import reactor.aeron.Context;
import reactor.core.state.Pausable;
import reactor.core.timer.Timer;
import reactor.core.util.Logger;

/**
 * @author Anatoly Kadyshev
 */
class HeartbeatWatchdog {

	private static final Logger logger = Logger.getLogger(HeartbeatWatchdog.class);

	private final ServiceMessageHandler serviceMessageHandler;

	private final SessionReaper sessionReaper;

	private final SessionTracker<? extends Session> sessionTracker;

	private volatile Pausable pausable;

	private final long heartbeatTimeoutNs;

	class SessionReaper implements Consumer<Long> {

		private final List<Session> heartbeatLostSessions = new ArrayList<>();

		@Override
		public void accept(Long value) {
			long now = System.nanoTime();
			for (Session session : sessionTracker.getSessions()) {
				if (session.getLastHeartbeatTimeNs() > 0 &&
						session.getLastHeartbeatTimeNs() - now > heartbeatTimeoutNs) {
					heartbeatLostSessions.add(session);
				}
			}

			for (int i = 0; i < heartbeatLostSessions.size(); i++) {
				String sessionId = heartbeatLostSessions.get(i).getSessionId();
				serviceMessageHandler.handleCancel(sessionId);
				logger.debug("Closed session with Id: {} due to a heartbeat loss", sessionId);
			}

			heartbeatLostSessions.clear();
		}

	}

	public HeartbeatWatchdog(Context context, ServiceMessageHandler serviceMessageHandler,
							 SessionTracker<? extends Session> sessionTracker) {
		this.serviceMessageHandler = serviceMessageHandler;
		this.sessionTracker = sessionTracker;
		this.sessionReaper = new SessionReaper();
		this.heartbeatTimeoutNs = TimeUnit.MILLISECONDS.toNanos(context.heartbeatIntervalMillis());
	}

	public void start() {
		this.pausable = Timer.global().schedule(sessionReaper, (heartbeatTimeoutNs * 3000) / 2);

		logger.debug("HeartbeatWatchdog started");
	}

	public void shutdown() {
		if (pausable != null) {
			pausable.cancel();
		}

		logger.debug("HeartbeatWatchdog shutdown");
	}

}
