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

import reactor.test.TestSubscriber;
import reactor.ipc.netty.util.SocketUtils;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author Anatoly Kadyshev
 */
public class AeronTestUtils {

	public static void setAeronEnvProps() {
		String bufferLength = String.valueOf(128 * 1024);
		System.setProperty(MediaDriver.DIRS_DELETE_ON_START_PROP_NAME, "true");

		System.setProperty(Configuration.TERM_BUFFER_LENGTH_PROP_NAME, bufferLength);
		System.setProperty(Configuration.TERM_BUFFER_MAX_LENGTH_PROP_NAME, bufferLength);
		System.setProperty(Configuration.COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, bufferLength);
		System.setProperty(Configuration.PUBLICATION_LINGER_PROP_NAME,
				String.valueOf(TimeUnit.MILLISECONDS.toNanos(500)));

		EmbeddedMediaDriverManager driverManager = EmbeddedMediaDriverManager.getInstance();
		driverManager.getDriverContext().threadingMode(ThreadingMode.SHARED);
		driverManager.setDeleteAeronDirsOnExit(true);
	}

	public static void awaitMediaDriverIsTerminated(Duration timeout) throws InterruptedException {
		TestSubscriber.await(timeout, "Aeron hasn't been shutdown properly",
				() -> EmbeddedMediaDriverManager.getInstance().isTerminated());
	}

	public static String availableLocalhostChannel() {
		return "udp://localhost:" + SocketUtils.findAvailableUdpPort();
	}

}
