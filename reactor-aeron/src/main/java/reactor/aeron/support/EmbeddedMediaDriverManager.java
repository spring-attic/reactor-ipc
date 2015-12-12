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

import reactor.Timers;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author Anatoly Kadyshev
 */
public class EmbeddedMediaDriverManager {

	private static final EmbeddedMediaDriverManager INSTANCE = new EmbeddedMediaDriverManager();

	public static final int RETRY_SHUTDOWN_MILLIS = 250;

	public static final long DRIVER_SHUTDOWN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

	private MediaDriver driver;

	private int counter = 0;

	private Aeron aeron;

	private AeronCounters aeronCounters;

	private MediaDriver.Context driverContext;

	private boolean terminated = true;

	private class RetryShutdownTask implements Consumer<Long> {

		private final long startNs;

		private final Timer timer;

		public RetryShutdownTask(Timer timer) {
			this.startNs = System.nanoTime();
			this.timer = timer;
		}

		@Override
		public void accept(Long aLong) {
			if (canShutdownMediaDriver() || System.nanoTime() - startNs > DRIVER_SHUTDOWN_TIMEOUT_NS) {
				forceShutdown();
			} else {
				timer.submit(this, RETRY_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS);
			}
		}

		private boolean canShutdownMediaDriver() {
			final boolean canShutdownDriver[] = new boolean[] { true };
			aeronCounters.forEach(new BiConsumer<Integer, String>() {
				@Override
				public void accept(Integer id, String label) {
					if (label.startsWith("sender pos") || label.startsWith("subscriber pos")) {
						canShutdownDriver[0] = false;
					}
				}
			});
			return canShutdownDriver[0];
		}
	}

	public static EmbeddedMediaDriverManager getInstance() {
		return INSTANCE;
	}

	public synchronized MediaDriver.Context getDriverContext() {
		if (driverContext == null) {
			driverContext = new MediaDriver.Context();
		}
		return driverContext;
	}

	public synchronized void launchDriver() {
		if (driver == null) {
			driver = MediaDriver.launchEmbedded(getDriverContext());
			Aeron.Context ctx = new Aeron.Context();
			ctx.aeronDirectoryName(driver.aeronDirectoryName());
			aeron = Aeron.connect(ctx);

			aeronCounters = new AeronCounters(driver.aeronDirectoryName());
			terminated = false;
		}
		counter++;
	}

	public synchronized void shutdownDriver() {
		counter = BackpressureUtils.subOrZero(counter, 1);
		if (counter == 0) {
			shutdown();
		}
	}

	public synchronized Aeron getAeron() {
		return aeron;
	}

	public synchronized int getCounter() {
		return counter;
	}

	public synchronized void shutdown() {
		counter = 0;

		if (driver != null) {
			aeron.close();

			Timer timer = Timers.global();
			timer.submit(new RetryShutdownTask(timer), RETRY_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Could result into JVM crashes when there is pending Aeron activity
	 */
	public synchronized void forceShutdown() {
		aeron = null;
		aeronCounters = null;
		driverContext = null;

		CloseHelper.quietClose(driver);
		driver = null;

		terminated = true;
	}

	public synchronized boolean isTerminated() {
		return terminated;
	}

}
