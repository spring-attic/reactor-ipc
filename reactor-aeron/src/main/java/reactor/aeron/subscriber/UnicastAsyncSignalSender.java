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
package reactor.aeron.subscriber;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.aeron.support.SignalPublicationFailedException;
import reactor.aeron.support.SignalType;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.Sequencer;
import reactor.core.util.ExecutorUtils;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * @author Anatoly Kadyshev
 */
public class UnicastAsyncSignalSender implements Runnable {

	private static final Logger logger = Logger.getLogger(UnicastAsyncSignalSender.class);

	private final Context context;

	private final SignalSender signalSender;

	private final ServiceMessageHandler serviceMessageHandler;

	private final Sequence pollCursor;

	private final ExecutorService executor;

	private final RingBuffer<Slot> buffer;

	private final Int2ObjectHashMap<FailedSessionData> dataBySessionUid = new Int2ObjectHashMap<>();

	private final List<FailedSessionData> expiredPublications = new ArrayList<>();

	//TODO: Make it configurable via Context
	private final long publicationTimeoutNs = TimeUnit.SECONDS.toNanos(2);

	private volatile boolean running = true;

	static class Slot {

		UnicastSession session;

		Buffer buffer;

		SignalType signalType;

		long lastPublicationTimeNs;

	}

	static class FailedSessionData {

		UnicastSession session;

		long lastPublicationTimeNs;

		Queue<Buffer> buffers = new ArrayDeque<>();

		Queue<SignalType> signalTypes = new ArrayDeque<>();

		boolean isEmpty() {
			return buffers.isEmpty();
		}

	}

	public UnicastAsyncSignalSender(AeronInfra aeronInfra, Context context,
									ServiceMessageHandler serviceMessageHandler) {
		this.context = context;
		this.signalSender = createSignalSender(aeronInfra, context);
		this.serviceMessageHandler = serviceMessageHandler;
		this.buffer = RingBuffer.createMultiProducer(new Supplier<Slot>() {
			@Override
			public Slot get() {
				return new Slot();
			}
		}, PlatformDependent.MEDIUM_BUFFER_SIZE);

		this.pollCursor = Sequencer.newSequence(-1L);
		buffer.addGatingSequence(pollCursor);
		this.executor = ExecutorUtils.singleUse("async-signal-sender", null);
	}

	protected SignalSender createSignalSender(AeronInfra aeronInfra, Context context) {
		return new SignalSender(aeronInfra, context.errorConsumer());
	}

	public void start() {
		logger.debug("Async signal sender started");

		running = true;

		this.executor.submit(this);
	}

	public void shutdown() {
		logger.debug("Async signal sender shutdown");

		running = false;

		executor.shutdown();
	}

	public void schedule(UnicastSession session, Buffer data, SignalType signalType) {
		session.setAsyncSenderModeOn(true);

		long seq = buffer.next();

		Slot slot = buffer.get(seq);
		slot.session = session;
		slot.buffer = data;
		slot.signalType = signalType;
		slot.lastPublicationTimeNs = System.nanoTime();

		buffer.publish(seq);
	}

	@Override
	public void run() {
		IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
		while (running) {
			collectNewFailedSignals();

			if (dataBySessionUid.size() > 0) {
				retryPublishing();

				handleExpiredPublications();

				idleStrategy.idle(1);
			} else {
				idleStrategy.idle(0);
			}
		}
	}

	private void handleExpiredPublications() {
		if (expiredPublications.size() > 0) {
			for (int i = 0; i < expiredPublications.size(); i++) {
				FailedSessionData data = expiredPublications.get(i);
				dataBySessionUid.remove(data.session.getSessionUid());

				serviceMessageHandler.handleCancel(data.session.getSessionId());

				context.errorConsumer().accept(
						new SignalPublicationFailedException(
								data.session.getSessionId(), null, null, null));
			}
			expiredPublications.clear();
		}
	}

	private void retryPublishing() {
		for (FailedSessionData data: dataBySessionUid.values()) {
			if (data.isEmpty()) {
				continue;
			}

			if (System.nanoTime() - data.lastPublicationTimeNs > publicationTimeoutNs) {
				expiredPublications.add(data);
				continue;
			}

			try {
				long result;
				do {
					SignalType signalType = data.signalTypes.peek();

					Publication publication = (signalType == SignalType.Error) ?
							data.session.getErrorPublication() : data.session.getPublication();

					Buffer buffer = data.buffers.peek();

					try {
						result = signalSender.publishSignal(data.session.getSessionId(), publication, buffer,
								signalType, false);
					} catch (Exception e) {
						expiredPublications.add(data);
						break;
					}

					if (result >= 0) {
						data.signalTypes.poll();
						data.buffers.poll();
						data.lastPublicationTimeNs = System.nanoTime();
					}
				} while (result < 0 && !data.isEmpty());
			} catch (IllegalStateException | IllegalArgumentException e) {
				//TODO: Provide the exception into errorConsumer of Context
			}
		}
	}

	private void collectNewFailedSignals() {
		if (pollCursor.get() < buffer.get()) {
			Slot slot = buffer.get(pollCursor.incrementAndGet());

			int sessionUid = slot.session.getSessionUid();
			FailedSessionData data = dataBySessionUid.get(sessionUid);
			if (data == null) {
				data = new FailedSessionData();
				dataBySessionUid.put(sessionUid, data);
			}
			data.session = slot.session;
			data.buffers.add(slot.buffer);
			data.signalTypes.add(slot.signalType);
			if (data.lastPublicationTimeNs == 0) {
				data.lastPublicationTimeNs = slot.lastPublicationTimeNs;
			}
		}
	}

}
