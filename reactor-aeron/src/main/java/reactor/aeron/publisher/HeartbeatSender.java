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
package reactor.aeron.publisher;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import reactor.aeron.Context;
import reactor.aeron.utils.HeartbeatPublicationFailureException;
import reactor.core.Cancellation;
import reactor.core.scheduler.Schedulers;

/**
 * @author Anatoly Kadyshev
 */
class HeartbeatSender {

	private volatile Cancellation cancellable;

	private final Task task;

	private final Context context;

	private class Task implements Runnable{

		private final ServiceMessageSender serviceMessageSender;

		private final Consumer<Throwable> heartbeatFailedConsumer;

		private int failuresCounter = 0;

		public Task(ServiceMessageSender serviceMessageSender, Consumer<Throwable> heartbeatFailedConsumer) {
			this.serviceMessageSender = serviceMessageSender;
			this.heartbeatFailedConsumer = heartbeatFailedConsumer;
		}

		@Override
		public void run() {
			boolean success = false;
			Throwable cause = null;
			try {
				success = serviceMessageSender.sendHeartbeat() >= 0;
			} catch (Exception e) {
				cause = e;
			}

			if (success) {
				failuresCounter = 0;
			} else {
				if (++failuresCounter == context.maxHeartbeatPublicationFailures()) {
					cancellable.dispose();

					heartbeatFailedConsumer.accept(new HeartbeatPublicationFailureException(cause));
				}
			}
		}
	}

	/**
	 *
	 * @param context
	 * @param serviceMessageSender
	 * @param heartbeatFailedConsumer run when failed to publish heartbeat
	 *                                {@link Context#maxHeartbeatPublicationFailures} times in a row
	 */
	public HeartbeatSender(Context context,
						   final ServiceMessageSender serviceMessageSender,
						   final Consumer<Throwable> heartbeatFailedConsumer) {
		this.context = context;
		this.task = new Task(serviceMessageSender, heartbeatFailedConsumer);
	}

	public void start() {
		if(cancellable != null){
			throw new IllegalStateException("Heartbeat sending task was already scheduled");
		}

		this.cancellable = Schedulers.timer().schedulePeriodically(task,
				context.heartbeatIntervalMillis(),
				context.heartbeatIntervalMillis(), TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		if (cancellable != null) {
			cancellable.dispose();
		}
		cancellable = null;
	}

}
