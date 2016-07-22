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

import reactor.ipc.buffer.Buffer;

/**
 * @author Anatoly Kadyshev
 */
public class SignalPublicationFailedException extends Exception {

	/**
	 * sessionId for which signals failed to be published
	 */
	private final String sessionId;

	private final Buffer buffer;

	private final SignalType signalType;

	public SignalPublicationFailedException(String sessionId, Buffer buffer, SignalType signalType, Throwable cause) {
		super(String.format("Failed to publish signal of type %s into session with Id: %s",
				signalType.name(), sessionId), cause);
		this.sessionId = sessionId;
		this.buffer = buffer;
		this.signalType = signalType;
	}

	public String getSessionId() {
		return sessionId;
	}

	public SignalType getSignalType() {
		return signalType;
	}

	public Buffer getBuffer() {
		return buffer;
	}

}
