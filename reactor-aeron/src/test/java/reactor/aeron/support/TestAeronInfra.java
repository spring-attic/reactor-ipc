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

import org.mockito.Mockito;
import reactor.aeron.subscriber.SignalSender;
import reactor.core.test.TestSubscriber;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class TestAeronInfra implements AeronInfra {

	public static class SignalData {

		public volatile Buffer lastBuffer;

		public volatile SignalType lastSignalType;

		volatile int signalCounter = 0;

		@Override
		public String toString() {
			return "SignalData["
					+ "signalCounter=" + signalCounter
					+ "]";
		}
	}

	public class TestSignalSender extends SignalSender {

		public TestSignalSender() {
			super(TestAeronInfra.this, Throwable::printStackTrace);
		}

		@Override
		public long publishSignal(String sessionId, Publication publication, Buffer buffer, SignalType signalType,
								  boolean retryPublication) {
			SignalData signalData = dataByPublication.get(publication);
			if (signalData == null) {
				signalData = new SignalData();
			}
			signalData.lastBuffer = buffer;
			signalData.lastSignalType = signalType;
			signalData.signalCounter++;

			dataByPublication.put(publication, signalData);

			System.out.println(signalData);

			return 1;
		}

	}

	/**
	 * key = channel/streamId
	 */
	private Map<String, Publication> publicationByKey = new ConcurrentHashMap<>();

	private Map<Publication, SignalData> dataByPublication = new ConcurrentHashMap<>();

	private TestSignalSender signalSender = new TestSignalSender();

	private volatile boolean shouldFailClaim = false;

	@Override
	public void initialise() {
	}

	@Override
	public void shutdown() {
	}

	@Override
	public Publication addPublication(String channel, int streamId) {
		Publication publication = Mockito.mock(Publication.class);
		publicationByKey.put(channel + "/" + streamId, publication);
		return publication;
	}

	@Override
	public Subscription addSubscription(String channel, int streamId) {
		return Mockito.mock(Subscription.class);
	}

	@Override
	public long claim(Publication publication, BufferClaim bufferClaim, int length, IdleStrategy idleStrategy,
					  boolean retryClaim) {
		if (shouldFailClaim) {
			return -1;
		} else {
			bufferClaim.wrap(new UnsafeBuffer(new byte[length + 128]), 0, length + 128);
			return 1;
		}
	}

	@Override
	public void close(Publication publication) {
	}

	@Override
	public void close(Subscription subscription) {
	}

	/**
	 * Returns data for the given key
	 *
	 * @param key a key
	 *
	 * @return null if no data was found for key
	 */
	public SignalData getSignalDataByKey(String key) {
		Publication publication = publicationByKey.get(key);
		SignalData signalData = null;
		if (publication != null) {
			signalData = dataByPublication.get(publication);
		}
		return signalData;
	}

	public void assertNextSignalPublished(String key, int numExpectedSignals) {
		SignalData signalData = getSignalDataByKey(key);
		assertThat(signalData.signalCounter, is(numExpectedSignals));
		signalData.signalCounter = 0;
	}

	public void waitNextSignalIsPublished(String key, int numExpectedSignals) throws InterruptedException {
		TestSubscriber.await(500, "No signals were published", new Supplier<Boolean>() {
			@Override
			public Boolean get() {
				SignalData signalData = getSignalDataByKey(key);
				return signalData != null && signalData.signalCounter == numExpectedSignals;
			}
		});
	}

	public TestSignalSender getSignalSender() {
		return signalSender;
	}

	public void setShouldFailClaim(boolean shouldFailClaim) {
		this.shouldFailClaim = shouldFailClaim;
	}

}
