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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WIP functionality
 *
 * @author Anatoly Kadyshev
 */
public class Stepper {

	private int current = -1;

	private int allowed = -1;

	private final Lock lock = new ReentrantLock();

	private final Condition currentCondition = lock.newCondition();

	private final Condition allowedCondition = lock.newCondition();

	private long timeoutNs = TimeUnit.SECONDS.toNanos(1000);

	public void await(int step) throws InterruptedException {
		lock.lock();
		try {
			if (current > step) {
				throw new IllegalArgumentException("current > step");
			}
			System.out.println(Thread.currentThread().getName() + " - Waiting for step " + step);
			long start = System.nanoTime();
			while (current < step) {
				currentCondition.await(1, TimeUnit.SECONDS);
				if (System.nanoTime() - start > timeoutNs) {
					throw new RuntimeException(Thread.currentThread().getName() +
							" - Step " + step + " hasn't been reached");
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void reached(int step) throws InterruptedException {
		lock.lock();
		try {
			if (current + 1 != step) {
				throw new IllegalArgumentException("Should expect step " + (current + 1));
			}
			System.out.println(Thread.currentThread().getName() + " - reached step " + step);

			current = step;
			currentCondition.signal();

			while (allowed < current) {
				allowedCondition.await(100, TimeUnit.MILLISECONDS);
			}

		} finally {
			lock.unlock();
		}
	}

	public void allow(int step) {
		lock.lock();
		try {
			if (current != step) {
				throw new IllegalArgumentException("step should be " + current);
			}

			allowed = current;
			allowedCondition.signal();
		} finally {
			lock.unlock();
		}
	}

	public void awaitAndAllow(int step) throws InterruptedException {
		await(step);
		allow(step);
	}

}
