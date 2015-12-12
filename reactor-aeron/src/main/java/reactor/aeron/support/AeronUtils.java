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
