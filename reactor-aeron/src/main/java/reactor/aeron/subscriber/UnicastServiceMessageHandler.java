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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.aeron.Context;
import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.SignalType;
import reactor.core.MultiProducer;
import reactor.util.Loggers;
import reactor.core.publisher.FluxProcessor;
import reactor.util.Logger;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;


/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
class UnicastServiceMessageHandler implements ServiceMessageHandler, MultiProducer {

	private static final Logger logger = Loggers.getLogger(UnicastServiceMessageHandler.class);

	/**
	 * Timeout for a subscription to be assigned into {@link InnerSubscriber}
	 * after it has been subscribed to the processor
	 */
	private static final long SUBSCRIPTION_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

	private final Processor<Buffer, Buffer> processor;

	private final AeronInfra aeronInfra;

	private final Context context;

	/**
	 * Run when a terminal event is published into all established sessions
	 */
	private final Runnable onTerminalEventTask;

	private final SessionTracker<UnicastSession> sessionTracker;

	private final HeartbeatWatchdog heartbeatWatchdog;

	private class InnerSubscriber implements Subscriber<Buffer> {

		private final SignalSender signalSender;

		private final UnicastSession session;

		InnerSubscriber(UnicastSession session) {
			this.session = session;
			this.signalSender = createSignalSender();
		}

		@Override
		public void onSubscribe(Subscription s) {
			session.subscription = s;

			long demand = session.getAndResetDemad();
			if (demand > 0) {
				session.subscription.request(demand);
			}
		}

		@Override
		public void onNext(Buffer buffer) {
			signalSender.publishSignal(session.getSessionId(), session.getPublication(), buffer, SignalType.Next, true);
		}

		@Override
		public void onError(Throwable t) {
			session.setTerminal();

			Buffer buffer = Buffer.wrap(context.exceptionSerializer().serialize(t));
			signalSender.publishSignal(session.getSessionId(), session.getPublication(), buffer,
					SignalType.Error, true);
		}

		@Override
		public void onComplete() {
			session.setTerminal();

			Buffer buffer = new Buffer(0, true);
			signalSender.publishSignal(session.getSessionId(), session.getPublication(), buffer,
					SignalType.Complete, true);
		}

	}

	public UnicastServiceMessageHandler(FluxProcessor<Buffer, Buffer> processor,
										AeronInfra aeronInfra,
										Context context,
										Runnable onTerminalEventTask) {
		this.processor = processor;
		this.aeronInfra = aeronInfra;
		this.context = context;
		this.onTerminalEventTask = onTerminalEventTask;
		this.sessionTracker = new BasicSessionTracker<>();
		this.heartbeatWatchdog = new HeartbeatWatchdog(context, this, sessionTracker);
	}

	protected SignalSender createSignalSender() {
		return new BasicSignalSender(aeronInfra, context.errorConsumer());
	}

	@Override
	public void handleMore(String sessionId, long n) {
		UnicastSession session = getOrCreateSession(sessionId);

		session.requestMore(n);
		if (session.subscription != null) {
			long demand = session.getAndResetDemad();
			if (demand > 0) {
				session.subscription.request(demand);
			}
		}
	}

	@Override
	public void handleHeartbeat(String sessionId) {
		UnicastSession session = getOrCreateSession(sessionId);
		session.setLastHeartbeatTimeNs(System.nanoTime());
	}

	@Override
	public void handleCancel(String sessionId) {
		UnicastSession session = sessionTracker.remove(sessionId);
		if (session != null) {
			boolean isTerminal = session.isTerminal();

			cancel(session);

			if ((isTerminal || context.autoCancel()) && sessionTracker.getSessionCounter() == 0) {
				onTerminalEventTask.run();
			}

		} else {
			logger.debug("Could not find a session to close with Id: {}", sessionId);
		}
	}

	@Override
	public void start() {
		heartbeatWatchdog.start();
	}

	@Override
	public void shutdown() {
		heartbeatWatchdog.shutdown();

		sendCompleteIntoNonTerminalSessions();
		closeAllSessions();
	}

	private void sendCompleteIntoNonTerminalSessions() {
		SignalSender signalSender = new BasicSignalSender(aeronInfra, context.errorConsumer());
		Buffer buffer = new Buffer(0, true);
		for (UnicastSession session: sessionTracker.getSessions()) {
			if (!session.isTerminal()) {
				session.setTerminal();
				signalSender.publishSignal(session.getSessionId(), session.getPublication(), buffer,
						SignalType.Complete, true);
			}
		}
	}

	private void closeAllSessions() {
		for (UnicastSession session: sessionTracker.getSessions()) {
			cancel(session);
		}
	}

	private UnicastSession createSession(String sessionId) {
		Matcher matcher = AeronUtils.UNICAST_SESSION_ID_PATTERN.matcher(sessionId);
		if (matcher.matches()) {
			String receiverChannel = matcher.group(1);

			boolean isValidChannel = false;
			try {
				isValidChannel = AeronUtils.isUnicastChannel(receiverChannel);
			} catch (Exception ex) {
			}
			if (!isValidChannel) {
				throw new IllegalArgumentException("Invalid unicast receiver channel: " + receiverChannel);
			}

			int streamId = Integer.parseInt(matcher.group(2));

			return new UnicastSession(
					sessionId,
					aeronInfra.addPublication(receiverChannel, streamId));
		} else {
			throw new IllegalArgumentException("Malformed unicast sessionId: " + sessionId);
		}
	}

	private void cancel(UnicastSession session) {
		if (waitTillSubscriptionIsAssigned(session)) {
			session.subscription.cancel();
		} else {
			context.errorConsumer().accept(
					new RuntimeException(String.format(
							"No subscription for inner subscriber for sessionId: %s was assigned during %d millis",
							session.getSessionId(), SUBSCRIPTION_TIMEOUT_NS)));
		}

		aeronInfra.close(session.getPublication());

		logger.debug("Closed session with sessionId: {}", session.getSessionId());
	}

	public boolean waitTillSubscriptionIsAssigned(UnicastSession session) {
		long start = System.nanoTime();
		if (session.subscription == null) {
			BackoffIdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
			while (session.subscription == null) {
				idleStrategy.idle(0);
				if (System.nanoTime() - start > SUBSCRIPTION_TIMEOUT_NS) {
					return false;
				}
			}
		}
		return true;
	}

	public UnicastSession getOrCreateSession(String sessionId) {
		UnicastSession session = sessionTracker.get(sessionId);
		if (session == null) {
			session = createSession(sessionId);
			sessionTracker.put(sessionId, session);

			processor.subscribe(new InnerSubscriber(session));

			logger.debug("New session established with sessionId: {}", sessionId);
		}
		return session;
	}

	@Override
	public Iterator<?> downstreams() {
		return sessionTracker.getSessions().iterator();
	}

	@Override
	public long downstreamCount() {
		return sessionTracker.getSessionCounter();
	}

}
