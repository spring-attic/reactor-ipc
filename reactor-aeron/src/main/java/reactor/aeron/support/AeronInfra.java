package reactor.aeron.support;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * @author Anatoly Kadyshev
 */
public interface AeronInfra {

	void initialise();

	void shutdown();

	Publication addPublication(String channel, int streamId);

	Subscription addSubscription(String channel, int streamId);

	long claim(Publication publication, BufferClaim bufferClaim, int length, IdleStrategy idleStrategy,
			   boolean retryClaim);

	void waitPublicationLinger();

	void waitSent(Publication publication, long position);

	void close(Publication publication);

	void close(Subscription subscription);

}
