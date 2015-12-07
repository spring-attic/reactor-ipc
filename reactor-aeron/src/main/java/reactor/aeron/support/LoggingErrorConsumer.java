package reactor.aeron.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Consumer;


/**
 * @author Anatoly Kadyshev
 */
public class LoggingErrorConsumer implements Consumer<Throwable> {

	private static final Logger logger = LoggerFactory.getLogger(LoggingErrorConsumer.class);

	@Override
	public void accept(Throwable t) {
		logger.error("Unexpected exception", t);
	}

}
