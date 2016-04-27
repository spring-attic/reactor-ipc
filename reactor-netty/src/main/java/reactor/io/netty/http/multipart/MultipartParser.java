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

package reactor.io.netty.http.multipart;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.util.Exceptions;

/**
 * @author Ben Hale
 */
final class MultipartParser extends SubscriberBarrier<MultipartTokenizer.Token, Flux<ByteBuf>> {

	private boolean done = false;

	private EmitterProcessor<ByteBuf> emitter;

	MultipartParser(Subscriber<? super Flux<ByteBuf>> subscriber) {
		super(subscriber);
	}

	@Override
	protected void doComplete() {
		if (this.done) {
			return;
		}

		this.done = true;

		if (this.emitter != null) {
			this.emitter.onComplete();
			this.emitter = null;
		}

		this.subscriber.onComplete();
	}

	@Override
	protected void doError(Throwable throwable) {
		if (this.done) {
			Exceptions.onErrorDropped(throwable);
			return;
		}

		this.done = true;

		if (this.emitter != null) {
			this.emitter.onError(throwable);
			this.emitter = null;
		}

		this.subscriber.onError(throwable);
	}

	@Override
	protected void doNext(MultipartTokenizer.Token token) {
		if (this.done) {
			Exceptions.onNextDropped(token);
			return;
		}

		switch (token.getKind()) {
			case BODY:
				if (this.emitter != null) {
					this.emitter.onNext(token.getByteBuf());
				}
				break;
			case CLOSE_DELIMITER:
				if (this.emitter != null) {
					this.emitter.onComplete();
					this.emitter = null;
				}

				this.done = true;
				this.subscription.cancel();
				this.subscriber.onComplete();
				break;
			case DELIMITER:
				if (this.emitter != null) {
					this.emitter.onComplete();
				}

				this.emitter = EmitterProcessor.<ByteBuf>create().connect();
				this.subscriber.onNext(this.emitter);
				break;
		}
	}

	@Override
	protected void doRequest(long n) {
		if (Integer.MAX_VALUE != n) {  // TODO: Support smaller request sizes
			onError(new IllegalArgumentException(
					"This operation only supports unbounded requests, was " + n));
		}
		else {
			super.doRequest(n);
		}
	}

}