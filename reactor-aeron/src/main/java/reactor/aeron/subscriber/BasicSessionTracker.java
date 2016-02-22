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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Anatoly Kadyshev
 */
class BasicSessionTracker<T extends Session> implements SessionTracker<T> {

	private final Map<String, T> sessionById = new ConcurrentHashMap<>();

	private volatile int sessionCounter = 0;

	@Override
	public T remove(String sessionId) {
		T prev = sessionById.remove(sessionId);
		if (prev != null) {
			sessionCounter--;
		}
		return prev;
	}

	@Override
	public T get(String sessionId) {
		return sessionById.get(sessionId);
	}

	@Override
	public void put(String sessionId, T sessionData) {
		if (sessionById.put(sessionId, sessionData) == null) {
			sessionCounter++;
		}
	}

	@Override
	public Collection<T> getSessions() {
		return sessionById.values();
	}

	public int getSessionCounter() {
		return sessionCounter;
	}

}

