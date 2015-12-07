package reactor.aeron.support;

import reactor.aeron.Context;
import uk.co.real_logic.aeron.driver.media.UdpChannel;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class AeronUtils {

	public final static Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	public static BackoffIdleStrategy newBackoffIdleStrategy() {
		return new BackoffIdleStrategy(
				100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
	}

	public static boolean isMulticastCommunication(Context context) {
		return UdpChannel.parse(context.receiverChannel()).isMulticast() ||
				context.senderChannel().equals(context.receiverChannel());
	}

	public static boolean isUnicastChannel(String channel) {
		return !UdpChannel.parse(channel).isMulticast();
	}

}
