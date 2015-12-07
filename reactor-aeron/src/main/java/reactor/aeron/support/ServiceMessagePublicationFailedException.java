package reactor.aeron.support;

/**
 * @author Anatoly Kadyshev
 */
public class ServiceMessagePublicationFailedException extends Exception {

	private final ServiceMessageType serviceMessageType;

	public ServiceMessagePublicationFailedException(ServiceMessageType serviceMessageType, Throwable cause) {
		super("Failed to publish service message type: " + serviceMessageType, cause);
		this.serviceMessageType = serviceMessageType;
	}

	public ServiceMessageType getServiceMessageType() {
		return serviceMessageType;
	}

}
