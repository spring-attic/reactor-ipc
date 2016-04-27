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

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.util.Exceptions;

/**
 * @author Ben Hale
 */
final class MultipartTokenizer
		extends SubscriberBarrier<ByteBuf, MultipartTokenizer.Token> {

	private static final char[] CRLF = new char[]{'\r', '\n'};

	private static final char[] DOUBLE_DASH = new char[]{'-', '-'};

	private final char[] boundary;

	private int bodyPosition;

	private int boundaryPosition;

	private ByteBuf byteBuf;

	private volatile boolean canceled = false;

	private int crlfPosition;

	private int delimiterPosition;

	private boolean done = false;

	private int doubleDashPosition;

	private int position;

	private Stage stage;

	MultipartTokenizer(String boundary, Subscriber<? super Token> subscriber) {
		super(subscriber);
		this.boundary = boundary.toCharArray();
		reset();
	}

	@Override
	protected void doCancel() {
		if (this.canceled) {
			return;
		}

		this.canceled = true;
		this.subscription.cancel();
	}

	@Override
	protected void doComplete() {
		if (this.done) {
			return;
		}

		this.done = true;
		this.subscriber.onComplete();
	}

	@Override
	protected void doError(Throwable throwable) {
		if (this.done) {
			Exceptions.onErrorDropped(throwable);
			return;
		}

		this.done = true;
		this.subscriber.onError(throwable);
	}

	@Override
	protected void doNext(ByteBuf byteBuf) {
		if (this.done) {
			Exceptions.onNextDropped(byteBuf);
			return;
		}

		this.byteBuf =
				this.byteBuf != null ? Unpooled.wrappedBuffer(this.byteBuf, byteBuf) :
						byteBuf;

		while (this.position < this.byteBuf.readableBytes()) {
			if (this.canceled) {
				break;
			}

			char c = getChar();

			switch (this.stage) {
				case BODY:
					body(c);
					break;
				case BOUNDARY:
					boundary(c);
					break;
				case END_CRLF:
					endCrLf(c);
					break;
				case END_DOUBLE_DASH:
					endDoubleDash(c);
					break;
				case START_CRLF:
					startCrLf(c);
					break;
				case START_DOUBLE_DASH:
					startDoubleDash(c);
					break;
				case TRAILING_CRLF:
					trailingCrLf(c);
					break;
			}
		}

		if (!this.canceled && Stage.BODY == this.stage) {
			pushTrailingBodyToken();
			reset();
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

	private void body(char c) {
		if (CRLF[0] == c) {
			this.delimiterPosition = this.position;
			this.stage = Stage.START_CRLF;
			this.crlfPosition = 1;
			this.position++;
		}
		else if (DOUBLE_DASH[0] == c) {
			this.delimiterPosition = this.position;
			this.stage = Stage.START_DOUBLE_DASH;
			this.doubleDashPosition = 1;
			this.position++;
		}
		else {
			this.position++;
		}
	}

	private void boundary(char c) {
		if (this.boundaryPosition < this.boundary.length) {
			if (this.boundary[this.boundaryPosition] == c) {
				this.boundaryPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			if (CRLF[0] == c) {
				this.stage = Stage.END_CRLF;
				this.crlfPosition = 1;
				this.position++;
			}
			else if (DOUBLE_DASH[0] == c) {
				this.stage = Stage.END_DOUBLE_DASH;
				this.doubleDashPosition = 1;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
	}

	private void endCrLf(char c) {
		if (this.crlfPosition < CRLF.length) {
			if (CRLF[this.crlfPosition] == c) {
				this.crlfPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			if (CRLF[0] == c) {
				this.stage = Stage.TRAILING_CRLF;
				this.crlfPosition = 1;
				this.position++;
			}
			else {
				pushBodyToken();
				pushDelimiterToken();
			}
		}
	}

	private void endDoubleDash(char c) {
		if (this.doubleDashPosition < DOUBLE_DASH.length) {
			if (DOUBLE_DASH[this.doubleDashPosition] == c) {
				this.doubleDashPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			pushBodyToken();
			pushCloseDelimiterToken();
		}
	}

	private char getChar() {
		return (char) (this.byteBuf.getByte(this.position) & 0xFF);
	}

	private void pushBodyToken() {
		pushToken(TokenKind.BODY, this.bodyPosition, this.delimiterPosition);
		this.bodyPosition = this.position;
	}

	private void pushCloseDelimiterToken() {
		pushToken(TokenKind.CLOSE_DELIMITER, this.delimiterPosition, this.position);
		this.stage = Stage.BODY;
	}

	private void pushDelimiterToken() {
		pushToken(TokenKind.DELIMITER, this.delimiterPosition, this.position);
		this.stage = Stage.BODY;
	}

	private void pushToken(TokenKind kind, int start, int end) {
		if (!this.canceled && (end - start > 0)) {
			this.subscriber.onNext(new Token(kind, this.byteBuf, start, end - start));
		}
	}

	private void pushTrailingBodyToken() {
		pushToken(TokenKind.BODY, this.bodyPosition, this.position);
	}

	private void reset() {
		this.bodyPosition = 0;
		this.byteBuf = null;
		this.position = 0;
		this.stage = Stage.BODY;
	}

	private void startCrLf(char c) {
		if (this.crlfPosition < CRLF.length) {
			if (CRLF[this.crlfPosition] == c) {
				this.crlfPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			if (DOUBLE_DASH[0] == c) {
				this.stage = Stage.START_DOUBLE_DASH;
				this.doubleDashPosition = 1;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
	}

	private void startDoubleDash(char c) {
		if (this.doubleDashPosition < DOUBLE_DASH.length) {
			if (DOUBLE_DASH[this.doubleDashPosition] == c) {
				this.doubleDashPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			if (this.boundary[0] == c) {
				this.stage = Stage.BOUNDARY;
				this.boundaryPosition = 1;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
	}

	private void trailingCrLf(char c) {
		if (this.crlfPosition < CRLF.length) {
			if (CRLF[this.crlfPosition] == c) {
				this.crlfPosition++;
				this.position++;
			}
			else {
				this.stage = Stage.BODY;
			}
		}
		else {
			pushBodyToken();
			pushDelimiterToken();
		}
	}

	private enum Stage {

		BODY,

		BOUNDARY,

		END_CRLF,

		END_DOUBLE_DASH,

		START_CRLF,

		START_DOUBLE_DASH,

		TRAILING_CRLF

	}

	static final class Token {

		private final ByteBuf byteBuf;

		private final TokenKind kind;

		private final int length;

		private final int offset;

		Token(TokenKind kind, ByteBuf byteBuf, int offset, int length) {
			this.byteBuf = byteBuf;
			this.kind = kind;
			this.length = length;
			this.offset = offset;
		}

		@Override
		public String toString() {
			return String.format("Token: %s, offset=%d, length=%d, '%s'",
					this.kind,
					this.offset,
					this.length,
					expandWhitespace(this.byteBuf.toString(this.offset,
							this.length,
							Charset.defaultCharset())));
		}

		ByteBuf getByteBuf() {
			return this.byteBuf.copy(this.offset, this.length);
		}

		TokenKind getKind() {
			return this.kind;
		}

		private static String expandWhitespace(String s) {
			return s.replaceAll("\r", "\\\\r")
			        .replaceAll("\n", "\\\\n")
			        .replaceAll("\t", "\\\\t");
		}

	}

	enum TokenKind {

		BODY,

		CLOSE_DELIMITER,

		DELIMITER

	}
}