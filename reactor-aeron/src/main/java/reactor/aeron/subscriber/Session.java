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

/**
 * A session between a signals sender and a signals receiver
 *
 * @author Anatoly Kadyshev
 */
interface Session {

	/**
	 * @return current session Id
     */
	String getSessionId();

	/**
	 * Increases requested demand for the session for n signals
	 *
	 * @param n number of signals to request
	 *
	 * @return a value indicating the previous demand
     */
	long requestMore(long n);

	/**
	 * @return time in nanos of the last heartbeat received for the session
     */
	long getLastHeartbeatTimeNs();

	/**
	 * Sets time of the last heartbeat received for the session in nanos
	 *
	 * @param lastHeartbeatTimeNs time of the last heartbeat in nanos
     */
	void setLastHeartbeatTimeNs(long lastHeartbeatTimeNs);

}
