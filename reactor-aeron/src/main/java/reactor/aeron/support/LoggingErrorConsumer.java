package reactor.aeron.support;

import reactor.core.support.Logger;
import reactor.core.support.Logger;
import reactor.fn.Consumer;


/**
 * @author Anatoly Kadyshev
 */
public class LoggingErrorConsumer implements Consumer<Throwable> {

	private static final Logger logger = Logger.getLogger(LoggingErrorConsumer.class);

	@Override
	public void accept(Throwable t) {
		logger.error("Unexpected exception", t);
	}

}
