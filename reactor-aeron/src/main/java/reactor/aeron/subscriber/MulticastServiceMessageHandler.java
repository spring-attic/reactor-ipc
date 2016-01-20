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

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.Serializer;
import reactor.aeron.support.SignalType;
import reactor.core.publisher.FluxProcessor;
import reactor.core.util.Logger;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;

/**
 * @author Anatoly Kadyshev
 */
public class MulticastServiceMessageHandler implements ServiceMessageHandler {

	private static final Logger logger = Logger.getLogger(MulticastServiceMessageHandler.class);

	private final Context context;

	/**
	 * Run when a terminal event is published into all established sessions
	 */
	private final Runnable shutdownTask;

	private final SessionTracker<MulticastSession> sessionTracker;

	/**
	 * Current sender sequence = (number of signals sent - 1)
	 */
	private volatile long cursor = -1L;

	/**
	 * Sequence for tracking number of requested signals from upstream = (number of requested signals - 1)
	 */
	private final AtomicLong requested = new AtomicLong(-1);

	/**
	 * Min of all session sequences tracking number of requested signals
	 */
	private volatile long minSequence = -1;

	private final HeartbeatWatchdog heartbeatWatchdog;

	private final InnerSubscriber subscriber;

	class InnerSubscriber implements Subscriber<Buffer> {

		private static final String SESSION_ID = "<multicast>";

		private final Publication nextCompletePub;

		private final Publication errorPub;

		private final Serializer<Throwable> exceptionSerializer;

		private final SignalSender signalSender;

		private volatile boolean terminal = false;

		private volatile Subscription subscription;

		InnerSubscriber(Context context,
						AeronInfra aeronInfra) {
			this.exceptionSerializer = context.exceptionSerializer();
			this.nextCompletePub = aeronInfra.addPublication(context.receiverChannel(), context.streamId());
			this.errorPub = aeronInfra.addPublication(context.receiverChannel(), context.errorStreamId());
			this.signalSender = new SignalSender(aeronInfra, context.errorConsumer());
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.subscription = s;

			requestFromUpstream();
		}

		@Override
		public void onNext(Buffer buffer) {
			signalSender.publishSignal(SESSION_ID, nextCompletePub, buffer, SignalType.Next, true);

			incrementCursor();
		}

		@Override
		public void onError(Throwable t) {
			Buffer buffer = Buffer.wrap(exceptionSerializer.serialize(t));

			signalSender.publishSignal(SESSION_ID, errorPub, buffer, SignalType.Error, true);

			terminal = true;
		}

		@Override
		public void onComplete() {
			Buffer buffer = new Buffer(0, true);

			signalSender.publishSignal(SESSION_ID, nextCompletePub, buffer, SignalType.Complete, true);

			terminal = true;
		}

		public boolean isTerminal() {
			return terminal;
		}

	}

	public MulticastServiceMessageHandler(FluxProcessor<Buffer, Buffer> processor,
										  AeronInfra aeronInfra,
										  Context context,
			Runnable shutdownTask) {
		this.context = context;
		this.shutdownTask = shutdownTask;
		this.sessionTracker = new BasicSessionTracker<>();
		this.heartbeatWatchdog = new HeartbeatWatchdog(context, this, sessionTracker);
		this.subscriber = new InnerSubscriber(context, aeronInfra);
		processor.subscribe(subscriber);
	}

	@Override
	public void handleMore(String sessionId, long n) {
		MulticastSession session = getOrCreateSession(sessionId);
		session.requestMore(n);

		minSequence = getMinSequence();

		if (subscriber.subscription == null) {
			return;
		}

		requestFromUpstream();
	}

	private void requestFromUpstream() {
		long requestedValue = requested.get();
		if (cursor == requestedValue &&  requestedValue < minSequence) {
			long toRequest = context.multicastUpstreamRequest();
			if (requested.compareAndSet(requestedValue, requestedValue + toRequest)) {
				subscriber.subscription.request(toRequest);
			}
		}
	}

	private long getMinSequence() {
		if (sessionTracker.getSessionCounter() == 0) {
			return cursor;
		}

		long minSequence = Long.MAX_VALUE;
		for (MulticastSession session: sessionTracker.getSessions()) {
			minSequence = Math.min(session.getSequence(), minSequence);
		}
		return minSequence;
	}

	public void incrementCursor() {
		cursor++;
		requestFromUpstream();
	}

	public MulticastSession getOrCreateSession(String sessionId) {
		MulticastSession session = sessionTracker.get(sessionId);
		if (session == null) {
			session = new MulticastSession(sessionId, cursor);
			sessionTracker.put(sessionId, session);

			if (logger.isDebugEnabled()) {
				logger.debug("New session established with sessionId: {}", sessionId);
			}
		}
		return session;
	}

	@Override
	public void handleHeartbeat(String sessionId) {
		MulticastSession session = getOrCreateSession(sessionId);
		session.setLastHeartbeatTimeNs(System.nanoTime());
	}

	@Override
	public void handleCancel(String sessionId) {
		MulticastSession session = sessionTracker.remove(sessionId);
		if (session != null) {
			minSequence = getMinSequence();

			if (logger.isDebugEnabled()) {
				logger.debug("Closed session with sessionId: {}", session.getSessionId());
			}

			if (sessionTracker.getSessionCounter() == 0) {
				if (context.autoCancel()) {
					subscriber.subscription.cancel();
				}

				if (context.autoCancel() || subscriber.isTerminal()) {
					shutdownTask.run();
				}
			}
		} else {
			//TODO: Handle
		}
	}

	@Override
	public void start() {
		heartbeatWatchdog.start();
	}

	@Override
	public void shutdown() {
		heartbeatWatchdog.shutdown();
	}

	protected long getCursor() {
		return cursor;
	}

}
