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
import org.reactivestreams.Subscription;
import reactor.aeron.support.DemandTracker;
import reactor.aeron.support.ServiceMessagePublicationFailedException;
import reactor.aeron.support.ServiceMessageType;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.io.buffer.Buffer;

/**
 * @author Anatoly Kadyshev
 */
class AeronPublisherSubscription implements Subscription, ReactiveState.Upstream, ReactiveState.Downstream,
                                            ReactiveState.DownstreamDemand, ReactiveState.ActiveUpstream,
                                            ReactiveState.ActiveDownstream, ReactiveState.Inner {

	private final Subscriber<? super Buffer> subscriber;

	private volatile boolean active = true;

	private final DemandTracker demandTracker;

	private final ServiceMessageSender serviceMessageSender;

	AeronPublisherSubscription(Subscriber<? super Buffer> subscriber, ServiceMessageSender serviceMessageSender) {
		this.subscriber = subscriber;
		this.demandTracker = new DemandTracker();
		this.serviceMessageSender = serviceMessageSender;
	}

	@Override
	public void request(long n) {
		if (active && BackpressureUtils.checkRequest(n, subscriber)) {
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
		active = false;
	}

	public DemandTracker getDemandTracker() {
		return demandTracker;
	}

	@Override
	public boolean isStarted() {
		return active;
	}

	@Override
	public boolean isTerminated() {
		return !active;
	}

	@Override
	public boolean isCancelled() {
		return !active;
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
	public Object upstream() {
		return serviceMessageSender;
	}
}
