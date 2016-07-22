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

import reactor.aeron.subscriber.SignalSender;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestSignalSender implements SignalSender {

    private final Map<String, PublicationData> dataBySessionId = new ConcurrentHashMap<>();

	public static class PublicationData {

		/**
		 * Last signal published
		 */
		public volatile Buffer lastSignal;

		/**
		 * Type of the last signal published
		 */
		public volatile SignalType lastSignalType;

		volatile int signalCounter = 0;
        @Override
		public String toString() {
			return "PublicationData["
					+ "signalCounter=" + signalCounter
					+ "]";
		}

	}

    @Override
    public long publishSignal(String sessionId, Publication publication, Buffer buffer, SignalType signalType,
							  boolean retryPublication) {
        PublicationData data = dataBySessionId.get(sessionId);
        if (data == null) {
            data = new PublicationData();
        }
        data.lastSignal = buffer;
        data.lastSignalType = signalType;
        data.signalCounter++;

        dataBySessionId.put(sessionId, data);
        return 1;
    }

    /**
	 * Returns publication data for the given sessionId
	 *
	 * @param sessionId a session Id
	 * @return null if no data was found for sessionId
	 */
    public PublicationData getPublicationDataBySessionId(String sessionId) {
        return dataBySessionId.get(sessionId);
    }

    public void assertNumSignalsPublished(String sessionId, int numExpectedSignals) {
        PublicationData publicationData = getPublicationDataBySessionId(sessionId);
        assertThat(publicationData.signalCounter, is(numExpectedSignals));
        publicationData.signalCounter = 0;
    }

}
