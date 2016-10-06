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

package reactor.ipc.connector;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Operators;
import reactor.ipc.Channel;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Allows registering Subscribers and Subscriptions for incoming messages
 * and allows sending messages.
 */

final class StreamEndpointImpl<IN, OUT> extends AtomicLong
		implements StreamEndpoint {

	static Logger log = Loggers.getLogger(StreamEndpointImpl.class);

	final ConcurrentMap<Long, Subscriber<OUT>> subscribers;

	final ConcurrentMap<Long, Subscription> subscriptions;

	final StreamRemote remote;

	final String name;

	final OnNewStream onNew;

	final Runnable onTerminate;

	final AtomicBoolean terminateOnce;

	final Channel<IN, OUT> channel;

	StreamEndpointImpl(String name,
			OnNewStream onNew,
			StreamRemote remote,
			Channel<IN, OUT> channel,
			Runnable onTerminate) {
		super(1);
		this.name = name;
		this.channel = channel;
		this.remote = remote;
		this.onNew = onNew;
		this.onTerminate = onTerminate;
		this.terminateOnce = new AtomicBoolean();
		this.subscribers = new ConcurrentHashMap<>();
		this.subscriptions = new ConcurrentHashMap<>();
	}

	long newStreamId() {
		return getAndIncrement();
	}

	void registerSubscription(long streamId, Subscription s) {
		if (subscriptions.putIfAbsent(streamId, s) != null) {
			throw new IllegalStateException("StreamID " + streamId + " already registered");
		}
	}

	void registerSubscriber(long streamId, Subscriber<OUT> s) {
		if (subscribers.putIfAbsent(streamId, s) != null) {
			throw new IllegalStateException("StreamID " + streamId + " already registered");
		}
	}

	boolean deregister(long streamId) {
		subscribers.remove(streamId);
		return subscriptions.remove(streamId) != null;
	}

	@Override
	public void onNew(long streamId, String function) {
		if (log.isDebugEnabled()) {
			log.debug("{}/onNew/{}/{}", name, streamId, function);
		}
		if (!onNew.onNew(streamId, function, this)) {
			if (log.isDebugEnabled()) {
				log.debug("{}/onNew/{} {}",
						name,
						streamId,
						"New stream(" + function + ") rejected");
			}
			sendCancel(streamId, "New stream(" + function + ") rejected");
		}
	}

	@Override
	public void onNext(long streamId, Object o) {

		if (log.isDebugEnabled()) {
			log.debug("{}/onNext/{}/value={}", name, streamId, o);
		}
		@SuppressWarnings("unchecked") Subscriber<Object> local =
				(Subscriber<Object>) subscribers.get(streamId);
		if (local != null) {
			try {
				local.onNext(o);
			}
			catch (Throwable ex) {
				if (log.isDebugEnabled()) {
					log.debug("{}/onNextError/{}/value={}", name, streamId, o, ex);
				}
				sendCancel(streamId, ex.toString());
				local.onError(ex);
			}
		}
	}

	@Override
	public void onError(long streamId, String reason) {
		onError(streamId, new Exception(reason));
	}

	@Override
	public void onError(long streamId, Throwable e) {
		if (log.isDebugEnabled()) {
			log.debug("{}/onError/{}", name, streamId, e);
		}
		if (streamId > 0) {
			Subscriber<OUT> local = subscribers.get(streamId);
			if (local != null) {
				local.onError(e);
				return;
			}
		}
		else if (streamId < 0) {
			if (terminateOnce.compareAndSet(false, true)) {
				onTerminate.run();
			}
			if (isClosed()) {
				return;
			}
		}
		Operators.onErrorDropped(e);
	}

	@Override
	public void onComplete(long streamId) {
		Subscriber<OUT> local = subscribers.get(streamId);
		if (local != null) {
			local.onComplete();
		}
	}

	@Override
	public void onCancel(long streamId, String reason) {
		if (log.isDebugEnabled()) {
			log.debug("{}/onCancel/{} {}", name, streamId, reason);
		}
		Subscription remove = subscriptions.get(streamId);
		if (remove != null) {
			remove.cancel();
		}
	}

	@Override
	public void onRequested(long streamId, long n) {
		if (log.isDebugEnabled()) {
			log.debug("{}/onRequested/{}/{}", name, streamId, n);
		}
		Subscription remote = subscriptions.get(streamId);
		if (remote != null) {
			remote.request(n);
		}
	}

	@Override
	public void sendNew(long streamId, String function) {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendNew/{}/{}", name, streamId, function);
		}
		remote.sendNew(streamId, function);
	}

	@Override
	public void sendCancel(long streamId, String reason) {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendCancel/{} {}", name, streamId, reason);
		}
		remote.sendCancel(streamId, reason);
	}

	@Override
	public void sendNext(long streamId, Object o) throws IOException {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendNext/{}/value={}", name, streamId, o);
		}
		remote.sendNext(streamId, o);
	}

	@Override
	public void sendError(long streamId, Throwable e) {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendError/{}", name, streamId, e);
		}
		remote.sendError(streamId, e);
	}

	@Override
	public void sendComplete(long streamId) {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendComplete/{}", name, streamId);
		}
		remote.sendComplete(streamId);
	}

	@Override
	public void sendRequested(long streamId, long n) {
		if (log.isDebugEnabled()) {
			log.debug("{}/sendRequested/{}/{}", name, streamId, n);
		}
		remote.sendRequested(streamId, n);
	}

	@Override
	public boolean isClosed() {
		return remote.isClosed();
	}
}
