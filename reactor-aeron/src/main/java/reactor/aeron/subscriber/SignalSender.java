package reactor.aeron.subscriber;

import reactor.aeron.utils.SignalType;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;

/**
 * @author Anatoly Kadyshev
 */
public interface SignalSender {

	long publishSignal(String sessionId, Publication publication, Buffer buffer, SignalType signalType,
					   boolean retryPublication);

}
