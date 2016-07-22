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

import org.reactivestreams.Subscriber;
import reactor.aeron.Context;
import reactor.aeron.utils.*;
import reactor.core.MultiProducer;
import reactor.core.Producer;
import reactor.util.Loggers;
import reactor.core.Trackable;
import reactor.core.publisher.Operators;
import reactor.core.Exceptions;
import reactor.util.Logger;
import reactor.ipc.buffer.Buffer;
import uk.co.real_logic.aeron.ControlledFragmentAssembler;
import uk.co.real_logic.aeron.logbuffer.ControlledFragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Signals receiver functionality which polls for signals sent by senders
 */
class SignalPoller implements org.reactivestreams.Subscription, Runnable, Producer,
                              Trackable, MultiProducer {

	private static final Logger logger = Loggers.getLogger(SignalPoller.class);

	private final AeronInfra aeronInfra;

	private final Runnable shutdownTask;

	private final DemandTracker demandTracker;

	private final Context context;

	private volatile boolean running;

	private final ServiceMessageSender serviceMessageSender;

	private boolean isLastSignalAborted = false;

	private final Serializer<Throwable> exceptionSerializer;

	private uk.co.real_logic.aeron.Subscription signalSub;

	protected final Subscriber<? super Buffer> subscriber;

	private final ControlledFragmentHandler fragmentAssembler = new ControlledFragmentAssembler(new ControlledFragmentHandler() {
		@Override
		public Action onFragment(DirectBuffer buffer, int offset, int length, Header header) {
			byte[] bytes = new byte[length - 1];
			buffer.getBytes(offset + 1, bytes);
			byte signalTypeCode = buffer.getByte(offset);
			Throwable error = null;
			try {
				if (signalTypeCode == SignalType.Next.getCode()) {
					if (demand > 0) {
						demand--;
						isLastSignalAborted = false;
						subscriber.onNext(Buffer.wrap(bytes));
					} else {
						isLastSignalAborted = true;
						return Action.ABORT;
					}
				} else if (signalTypeCode == SignalType.Complete.getCode()) {
					running = false;
					subscriber.onComplete();
				} else if (signalTypeCode == SignalType.Error.getCode()) {
					error = exceptionSerializer.deserialize(bytes);
				} else {
					error = Exceptions.propagate(new IllegalStateException(
							String.format("Received message with unknown signal type code of %d and length of %d",
									signalTypeCode, bytes.length)));
				}
			} catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				error = t;
			}

			if (error != null) {
				running = false;
				subscriber.onError(error);
			}
			return Action.COMMIT;
		}
	});

	public SignalPoller(Context context,
						ServiceMessageSender serviceMessageSender,
						Subscriber<? super Buffer> subscriber,
						AeronInfra aeronInfra,
						Runnable shutdownTask) {

		this.context = context;
		this.serviceMessageSender = serviceMessageSender;
		this.subscriber = subscriber;
		this.aeronInfra = aeronInfra;
		this.shutdownTask = shutdownTask;
		this.demandTracker = new DemandTracker();
		this.exceptionSerializer = context.exceptionSerializer();
	}

	long demand = 0;

	@Override
	public void run() {
		running = true;
		logger.debug("Signal poller started, sessionId: {}", serviceMessageSender.getSessionId());

		this.signalSub = aeronInfra.addSubscription(context.receiverChannel(), context.streamId());

		setSubscriberSubscription();

		final IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();
		try {
			while (running) {
				if (demand == 0) {
					demand = demandTracker.getAndReset();
				}

				int fragmentLimit = (int) Math.min(demand, context.signalPollerFragmentLimit());
				if (fragmentLimit == 0 && !isLastSignalAborted) {
					fragmentLimit = 1;
				}
				int fragmentsReceived = signalSub.controlledPoll(fragmentAssembler, fragmentLimit);
				idleStrategy.idle(fragmentsReceived);
			}
		} finally {
			aeronInfra.close(signalSub);

			logger.trace("about to execute shutdownTask");
			shutdownTask.run();
		}

		logger.debug("Signal poller shutdown, sessionId: {}", serviceMessageSender.getSessionId());
	}

	private void setSubscriberSubscription() {
		//TODO: Possible timing issue due to ServiceMessagePoller termination
		try {
			if(running) {
				subscriber.onSubscribe(this);
			}
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			subscriber.onError(t);
		}
	}

	public void shutdown() {
		running = false;
	}

	@Override
	public Iterator<?> downstreams() {
		String up1 = signalSub != null ? signalSub.channel()+"/"+ signalSub.streamId() :
				context.receiverChannel()+"/"+context.streamId();

		return Arrays.asList(up1, serviceMessageSender).iterator();
	}

	@Override
	public long downstreamCount() {
		return running ? 2 : 0;
	}

	@Override
	public boolean isCancelled() {
		return !running;
	}

	@Override
	public boolean isStarted() {
		return running;
	}

	@Override
	public boolean isTerminated() {
		return !running;
	}

	@Override
	public Object downstream() {
		return subscriber;
	}

	@Override
	public long requestedFromDownstream() {
		return demandTracker.current();
	}

	@Override
	public void request(long n) {
		if (running && Operators.checkRequest(n, subscriber)) {
			try {
				serviceMessageSender.sendRequest(n);
				demandTracker.request(n);
			} catch (Exception e) {
				subscriber.onError(new ServiceMessagePublicationFailedException(ServiceMessageType.Request, e));
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
