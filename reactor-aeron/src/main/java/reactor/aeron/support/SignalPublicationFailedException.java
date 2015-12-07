package reactor.aeron.support;

import reactor.io.buffer.Buffer;

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
