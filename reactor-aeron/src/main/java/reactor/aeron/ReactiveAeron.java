package reactor.aeron;

import reactor.aeron.processor.AeronProcessor;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;

/**
 * A class with static factory methods to create a subscriber/publisher or processor
 * for Reactive Streams over Aeron.
 *
 * @author Anatoly Kadyshev
 */
public enum ReactiveAeron {
	;

	/**
	 * Creates a new instance of {@link AeronSubscriber}
	 * @param context configuration parameters
	 * @return a new instance of subscriber
	 */
	public static AeronSubscriber aeronSubscriber(Context context) {
		return AeronSubscriber.create(context);
	}

	/**
	 * Creates a new instance of {@link AeronPublisher}
	 * @param context configuration parameters
	 * @return a new instance of publisher
	 */
	public static AeronPublisher aeronPublisher(Context context) {
		return AeronPublisher.create(context);
	}

	/**
	 * Creates a new instance of {@link AeronProcessor}
	 * @param context configuration parameters
	 * @return a new instnace of processor
	 */
	public static AeronProcessor aeronProcessor(Context context) {
		return AeronProcessor.create(context);
	}

}
