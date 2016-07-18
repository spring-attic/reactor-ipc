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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import reactor.util.Loggers;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.IoUtil;

/**
 * @author Anatoly Kadyshev
 */
public final class EmbeddedMediaDriverManager {

	private static final Logger logger = Loggers.getLogger(EmbeddedMediaDriverManager.class);

	private static final EmbeddedMediaDriverManager INSTANCE = new EmbeddedMediaDriverManager();

	private final List<String> aeronDirNames = new ArrayList<>();

	private Thread shutdownHook;

	enum State {

		NOT_STARTED,

		STARTED,

		SHUTTING_DOWN
	}

	public static final long DEFAULT_RETRY_SHUTDOWN_MILLIS = 250;

	public static final long DEFAULT_SHUTDOWN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

	private long retryShutdownMillis = DEFAULT_RETRY_SHUTDOWN_MILLIS;

	/**
	 * Timeout in nanos to wait until forcefully shutting down the embedded Media driver
	 */
	private long shutdownTimeoutNs = DEFAULT_SHUTDOWN_TIMEOUT_NS;

	private MediaDriver driver;

	private int counter = 0;

	private Aeron aeron;

	private AeronCounters aeronCounters;

	private MediaDriver.Context driverContext;

	private State state = State.NOT_STARTED;

	/**
	 * If true then the Media driver is shutdown when the last functionality which
	 * started it is shutdown
	 */
	private boolean shouldShutdownWhenNotUsed = true;

	/**
	 * If true then deletes previously created Aeron dirs upon JVM termination
	 */
	private boolean deleteAeronDirsOnExit = false;

	private class RetryShutdownTask implements Runnable {

		private final long startNs;

		private final TimedScheduler timer;

		public RetryShutdownTask(TimedScheduler timer) {
			this.startNs = System.nanoTime();
			this.timer = timer;
		}

		@Override
		public void run() {
			if (canShutdownMediaDriver() || System.nanoTime() - startNs > shutdownTimeoutNs) {
				doShutdown();
			} else {
				timer.schedule(this, retryShutdownMillis, TimeUnit.MILLISECONDS);
			}
		}

		private boolean canShutdownMediaDriver() {
			final boolean canShutdownDriver[] = new boolean[] { true };
			aeronCounters.forEach((id, label) -> {
				if (label.startsWith(AeronUtils.LABEL_PREFIX_SENDER_POS) || label.startsWith(AeronUtils.LABEL_PREFIX_SUBSCRIBER_POS)) {
					canShutdownDriver[0] = false;
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
		if (state == State.SHUTTING_DOWN) {
			throw new IllegalStateException("Manager is being shutdown");
		}

		if (driver == null) {
			driver = MediaDriver.launchEmbedded(getDriverContext());
			Aeron.Context ctx = new Aeron.Context();
			String aeronDirName = driver.aeronDirectoryName();
			ctx.aeronDirectoryName(aeronDirName);
			aeron = Aeron.connect(ctx);

			aeronCounters = new AeronCounters(aeronDirName);
			state = State.STARTED;

			aeronDirNames.add(aeronDirName);

			logger.info("Embedded media driver started");
		}
		counter++;
	}

	public synchronized void shutdownDriver() {
		counter = (int)Operators.subOrZero(counter, 1);
		if (counter == 0 && shouldShutdownWhenNotUsed) {
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
		if (state != State.STARTED) {
			throw new IllegalStateException("Cannot shutdown manager in state: " + state);
		}
		state = State.SHUTTING_DOWN;

		counter = 0;

		if (driver != null) {
			aeron.close();

			TimedScheduler timer = Schedulers.timer();

			timer.schedule(new RetryShutdownTask(timer),
					retryShutdownMillis,
					TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Could result into JVM crashes when there is pending Aeron activity
	 */
	private synchronized void doShutdown() {
		aeron = null;
		try {
			aeronCounters.shutdown();
		} catch (Throwable t) {
			logger.error("Failed to shutdown Aeron counters", t);
		}
		aeronCounters = null;

		CloseHelper.quietClose(driver);

		driverContext = null;
		driver = null;

		state = State.NOT_STARTED;

		setupShutdownHook();

		logger.info("Embedded media driver shutdown");
	}

	private void setupShutdownHook() {
		if (!deleteAeronDirsOnExit || shutdownHook != null) {
			return;
		}

		shutdownHook = new Thread() {

			@Override
			public void run() {
				synchronized (EmbeddedMediaDriverManager.this) {
					for (String aeronDirName : aeronDirNames) {
						try {
							File dirFile = new File(aeronDirName);
							IoUtil.delete(dirFile, false);
						} catch (Exception e) {
							logger.error("Failed to delete Aeron directory: {}", aeronDirName);
						}
					}
				}
			}

		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

	public synchronized boolean isTerminated() {
		return state == State.NOT_STARTED;
	}

	public void setShouldShutdownWhenNotUsed(boolean shouldShutdownWhenNotUsed) {
		this.shouldShutdownWhenNotUsed = shouldShutdownWhenNotUsed;
	}

	public void setDeleteAeronDirsOnExit(boolean deleteAeronDirsOnExit) {
		this.deleteAeronDirsOnExit = deleteAeronDirsOnExit;
	}

	public void setShutdownTimeoutNs(long shutdownTimeoutNs) {
		this.shutdownTimeoutNs = shutdownTimeoutNs;
	}

}
