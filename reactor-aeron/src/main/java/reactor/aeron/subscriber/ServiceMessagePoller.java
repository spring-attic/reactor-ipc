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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import reactor.aeron.Context;
import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.ServiceMessageType;
import reactor.util.Loggers;
import reactor.core.Receiver;
import reactor.util.Logger;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * @author Anatoly Kadyshev
 */
class ServiceMessagePoller implements Runnable, Receiver {

	private static final Logger logger = Loggers.getLogger(ServiceMessagePoller.class);

	private final Subscription serviceRequestSub;

	private final Context context;

	private final ServiceMessageHandler serviceMessageHandler;

	private final ExecutorService executor;

	private final AeronInfra aeronInfra;

	private volatile boolean running;

	private volatile boolean terminated = false;

	private class PollerFragmentHandler implements FragmentHandler {

		private byte[] dst = new byte[255];

		@Override
		public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
			if (!running) {
				return;
			}

			byte type = buffer.getByte(offset);
			if (type == ServiceMessageType.Request.getCode()) {
				handleRequest(buffer, offset);
			} else if (type == ServiceMessageType.Cancel.getCode()) {
				handleCancel(buffer, offset);
			} else if (type == ServiceMessageType.Heartbeat.getCode()) {
				handleHeartbeat(buffer, offset, header);
			} else {
				logger.error("Unknown type code: {} received", type);
			}
		}

		private void handleHeartbeat(DirectBuffer buffer, int offset, Header header) {
			String sessionId = readSessionId(buffer, offset + 1);

			serviceMessageHandler.handleHeartbeat(sessionId);
		}

		private void handleCancel(DirectBuffer buffer, int offset) {
			String sessionId = readSessionId(buffer, offset + 1);

			if (logger.isTraceEnabled()) {
				logger.trace("Cancel request received for sessionId: {}", sessionId);
			}

			serviceMessageHandler.handleCancel(sessionId);
		}

		private void handleRequest(DirectBuffer buffer, int offset) {
			long n = buffer.getLong(offset + 1);
			String sessionId = readSessionId(buffer, offset + 1 + 8);

			if (logger.isTraceEnabled()) {
				logger.trace("Requested {} signals for sessionId: {}", n, sessionId);
			}

			serviceMessageHandler.handleMore(sessionId, n);
		}

		//TODO: Move to a better GC-free alternative for sessionId
		private String readSessionId(DirectBuffer buffer, int offset) {
			int length = buffer.getByte(offset) & 0xFF;
			buffer.getBytes(offset + 1, dst, 0, length);
			return new String(dst, 0, length, AeronUtils.UTF_8_CHARSET);
		}

	}

	ServiceMessagePoller(Context context, AeronInfra aeronInfra, ServiceMessageHandler serviceMessageHandler) {
		this.context = context;
		this.serviceMessageHandler = serviceMessageHandler;
		this.aeronInfra = aeronInfra;
		this.serviceRequestSub = aeronInfra.addSubscription(context.senderChannel(), context.serviceRequestStreamId());
		this.executor = Executors.newCachedThreadPool(
				r -> new Thread(r, AeronUtils.makeThreadName(
						context.name(),
						"subscriber",
						"service-poller")));
	}

	void start() {
		executor.execute(this);
	}

	public void run() {
		this.running = true;
		logger.debug("Service message poller started");

		IdleStrategy idleStrategy = AeronUtils.newBackoffIdleStrategy();

		FragmentAssembler fragmentAssembler = new FragmentAssembler(new PollerFragmentHandler());

		final int fragmentLimit = context.serviceMessagePollerFragmentLimit();

		while (running) {
			int nFragmentsReceived = 0;
			try {
				nFragmentsReceived = serviceRequestSub.poll(fragmentAssembler, fragmentLimit);
			} catch (Exception e) {
				context.errorConsumer().accept(e);
			}
			idleStrategy.idle(nFragmentsReceived);
		}

		aeronInfra.close(serviceRequestSub);

		logger.debug("Service message poller shutdown");
		terminated = true;
	}

	void shutdown() {
		this.running = false;

		executor.shutdown();
	}

	@Override
	public Object upstream() {
		return serviceRequestSub.channel()+"/"+serviceRequestSub.streamId();
	}

	public boolean isTerminated() {
		return terminated;
	}
}
