/*
 * Copyright (c) 2011-2016 Pivotal Software, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.aeron.utils;

import uk.co.real_logic.aeron.CncFileDescriptor;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.function.BiConsumer;

/**
 * Based on <a href="https://github.com/real-logic/Aeron/blob/master/aeron-samples/src/main/java/uk/co/real_logic/aeron/samples/AeronStat.java">AeronCounters.java from Aeron</a>
 */
public final class AeronCounters {

	private final CountersReader counters;

	private final MappedByteBuffer cncByteBuffer;

	public AeronCounters(String dirName) {
		final File cncFile = new File(dirName + "/cnc");

		cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
		final DirectBuffer metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
		final int cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

		if (CncFileDescriptor.CNC_VERSION != cncVersion) {
			throw new IllegalStateException("CNC version not supported: version=" + cncVersion);
		}

		AtomicBuffer labelsBuffer = CncFileDescriptor.createCountersMetaDataBuffer(cncByteBuffer, metaDataBuffer);
		AtomicBuffer valuesBuffer = CncFileDescriptor.createCountersValuesBuffer(cncByteBuffer, metaDataBuffer);
		counters = new CountersReader(labelsBuffer, valuesBuffer);
	}

	public void shutdown() {
		IoUtil.unmap(cncByteBuffer);
	}

	public void forEach(BiConsumer<Integer, String> consumer) {
		counters.forEach(consumer);
	}

	public long getCounterValue(int counterId) {
		return counters.getCounterValue(counterId);
	}

}
