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

package reactor.ipc.socket;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import reactor.ipc.stream.StreamOperations;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * Protocol pattern:
 * <p>
 * <pre>
 * 00-03: payload length including the header (4 bytes little endian)
 * 04-04: entry type (1 bytes)
 * 05-07: entry flags (3 bytes little endian)
 * 08-0F: stream identifier (8 bytes little endian); bit 63: set for server opened streams
 */
abstract class ByteArrayStreamProtocol {

	static final Logger log = Loggers.getLogger(ByteArrayStreamProtocol.class);

	/**
	 * Starts a new stream numbered by the sender, the payload
	 * is an UTF-8 function name if present
	 */
	public static final int  TYPE_NEW       = 1;
	/**
	 * Cancels and stops a stream, dropping all further messages
	 * with the same identifier. The payload, if present may
	 * contain a reason (UTF-8 error message with stacktrace).
	 */
	public static final int  TYPE_CANCEL    = 2;
	/**
	 * The next value within a stream.
	 */
	public static final int  TYPE_NEXT      = 3;
	/**
	 * The error signal. The payload, if present may
	 * contain a reason (UTF-8 error message with stacktrace).
	 */
	public static final int  TYPE_ERROR     = 4;
	/**
	 * The complete signal, stopping a stream.
	 */
	public static final int  TYPE_COMPLETE  = 5;
	/**
	 * Indicate more values can be sent. If no payload present,
	 * the flags holds the 3 byte positive integer amount,
	 * if payload present, that indicates the request amount. Integer.MAX_VALUE and
	 * negative amounts indicate unbounded mode. Zero is ignored in both cases.
	 */
	public static final int  TYPE_REQUEST   = 6;
	public static final byte PAYLOAD_OBJECT = 0;
	public static final byte PAYLOAD_INT    = 1;
	public static final byte PAYLOAD_LONG   = 2;
	public static final byte PAYLOAD_STRING = 3;
	public static final byte PAYLOAD_BYTES  = 4;

	public static void cancel(OutputStream out, long streamId, String reason, byte[] wb) {
		send(out, streamId, TYPE_CANCEL, 0, utf8(reason), wb);
	}

	public static void cancel(OutputStream out,
			long streamId,
			Throwable reason,
			byte[] wb) {
		send(out, streamId, TYPE_CANCEL, 0, errorBytes(reason), wb);
	}

	public static void complete(OutputStream out, long streamId, byte[] wb) {
		send(out, streamId, TYPE_COMPLETE, 0, EMPTY, wb);
	}

	public static void error(OutputStream out, long streamId, String reason, byte[] wb) {
		send(out, streamId, TYPE_ERROR, 0, utf8(reason), wb);
	}

	public static void error(OutputStream out,
			long streamId,
			Throwable reason,
			byte[] wb) {
		send(out, streamId, TYPE_ERROR, 0, errorBytes(reason), wb);
	}

	public static byte[] errorBytes(Throwable reason) {
		if (reason == null) {
			return EMPTY;
		}
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		reason.printStackTrace(new PrintWriter(new OutputStreamWriter(bout,
				StandardCharsets.UTF_8)));
		return bout.toByteArray();
	}

	public static void next(OutputStream out,
			long streamId,
			int flags,
			byte[] data,
			byte[] wb) {
		send(out, streamId, TYPE_NEXT, flags, data, wb);
	}

	public static void next(OutputStream out,
			long streamId,
			int flags,
			String text,
			byte[] wb) {
		next(out, streamId, flags, utf8(text), wb);
	}

	public static void open(OutputStream out,
			long streamId,
			String functionName,
			byte[] wb) {
		send(out, streamId, TYPE_NEW, 0, utf8(functionName), wb);
	}

	static int readFully(InputStream in, byte[] output, int count) throws IOException {
		int offset = 0;
		int remaining = count;

		for (; ; ) {
			int a = in.read(output, offset, remaining);
			if (a < 0) {
				break;
			}
			offset += a;
			remaining -= a;
			if (remaining == 0) {
				break;
			}
		}

		return offset;
	}

	static String readUtf8(InputStream in, int payloadLength) throws IOException {
		StringBuilder sb = new StringBuilder(payloadLength);

		for (; ; ) {
			if (payloadLength == 0) {
				break;
			}
			int b = in.read();

			payloadLength--;

			if (b < 0) {
				break;
			}
			else if ((b & 0x80) == 0) {
				sb.append((char) b);
			}
			else if ((b & 0b1110_0000) == 0b1100_0000) {

				if (payloadLength-- == 0) {
					break;
				}

				int b1 = in.read();
				if (b1 < 0) {
					break;
				}

				int c = ((b & 0b1_1111) << 6) | (b1 & 0b0011_1111);
				sb.append((char) c);
			}
			else if ((b & 0b1111_0000) == 0b1110_0000) {
				if (payloadLength-- == 0) {
					break;
				}

				int b1 = in.read();

				if (b1 < 0) {
					break;
				}

				if (payloadLength-- == 0) {
					break;
				}

				int b2 = in.read();

				if (b2 < 0) {
					break;
				}

				int c =
						((b & 0b1111) << 12) | ((b1 & 0b11_1111) << 6) | ((b2 & 0b11_1111));

				sb.append((char) c);
			}
			else if ((b & 0b1111_1000) == 0b1111_0000) {
				if (payloadLength-- == 0) {
					break;
				}

				int b1 = in.read();

				if (b1 < 0) {
					break;
				}

				if (payloadLength-- == 0) {
					break;
				}

				int b2 = in.read();

				if (b2 < 0) {
					break;
				}

				if (payloadLength-- == 0) {
					break;
				}

				int b3 = in.read();

				if (b3 < 0) {
					break;
				}

				int c =
						((b & 0b111) << 18) | ((b1 & 0b11_1111) << 12) | ((b1 & 0b11_1111) << 6) | ((b2 & 0b11_1111));

				sb.append((char) c);
			}
		}

		return sb.toString();
	}

	static String readUtf8(byte[] in, int start, int len) throws IOException {
		StringBuilder sb = new StringBuilder(len);

		for (; ; ) {
			if (start == len) {
				break;
			}
			int b = in[start++] & 0xFF;

			if ((b & 0x80) == 0) {
				sb.append((char) b);
			}
			else if ((b & 0b1110_0000) == 0b1100_0000) {

				if (start == len) {
					break;
				}

				int b1 = in[start++] & 0b11_1111;

				int c = ((b & 0b1_1111) << 6) | (b1);
				sb.append((char) c);
			}
			else if ((b & 0b1111_0000) == 0b1110_0000) {
				if (start == len) {
					break;
				}
				int b1 = in[start++] & 0b11_1111;

				if (start == len) {
					break;
				}

				int b2 = in[start++] & 0b11_1111;

				int c = ((b & 0b1111) << 12) | (b1 << 6) | (b2);

				sb.append((char) c);
			}
			else if ((b & 0b1111_1000) == 0b1111_0000) {
				if (start == len) {
					break;
				}
				int b1 = in[start++] & 0b11_1111;

				if (start == len) {
					break;
				}

				int b2 = in[start++] & 0b11_1111;

				if (start == len) {
					break;
				}

				int b3 = in[start++] & 0b11_1111;

				int c = ((b & 0b111) << 18) | (b1 << 12) | (b2 << 6) | (b3);

				sb.append((char) c);
			}
		}

		return sb.toString();
	}

	public static boolean receive(InputStream in, byte[] rb, StreamOperations onReceive) {
		try {

			int len =
					(rb[0] & 0xFF) | ((rb[1] & 0xFF) << 8) | ((rb[2] & 0xFF) << 16) | ((rb[3] & 0xFF) << 24);

			byte type = rb[4];

			int flags = ((rb[5] & 0xFF)) | ((rb[6] & 0xFF) << 8) | ((rb[7] & 0xFF) << 16);

			long streamId =
					(rb[8] & 0xFFL) | ((rb[9] & 0xFFL) << 8) | ((rb[10] & 0xFFL) << 16) | ((rb[11] & 0xFFL) << 24) | ((rb[12] & 0xFFL) << 32) | ((rb[13] & 0xFFL) << 40) | ((rb[14] & 0xFFL) << 48) | ((rb[15] & 0xFFL) << 56);

			switch (type) {
				case TYPE_NEW: {
					len -= 16;
					if (len != 0) {
						String function;
						if (len <= rb.length) {
							int r = readFully(in, rb, len);
							if (r < len) {
								onReceive.onError(streamId,
										"Channel/Connection closed (@ new)");
								return false;
							}
							function = readUtf8(rb, 0, len);
						}
						else {
							function = readUtf8(in, len);
						}
						onReceive.onNew(streamId, function);
					}
					else {
						onReceive.onNew(streamId, "");
					}
					break;
				}
				case TYPE_CANCEL: {
					if (len > 16) {
						String reason = readUtf8(in, len - 16);
						onReceive.onCancel(streamId, reason);
					}
					else {
						onReceive.onCancel(streamId, "");
					}
					break;
				}

				case TYPE_NEXT: {
					len -= 16;
					if (len != 0) {
						byte[] payload;
						if (len <= rb.length) {
							payload = rb;
						}
						else {
							payload = new byte[len];
						}
						int r = readFully(in, payload, len);
						onNext(streamId, flags, payload, len, r, onReceive, rb);
					}
					else {
						onNext(streamId, flags, EMPTY, 0, 0, onReceive, rb);
					}
					break;
				}
				case TYPE_ERROR: {
					if (len > 16) {
						String reason = readUtf8(in, len - 16);
						onReceive.onError(streamId, reason);
					}
					else {
						onReceive.onError(streamId, "");
					}
					break;
				}

				case TYPE_COMPLETE: {
					// ignore payload
					len -= 16;
					while (len != 0) {
						int r = in.read(rb, 0, Math.min(len, rb.length));
						if (r < 0) {
							break;
						}
						len -= r;
					}
					onReceive.onComplete(streamId);
					break;
				}

				case TYPE_REQUEST: {
					if (len > 16) {
						if (readFully(in, rb, 8) < 8) {
							onReceive.onError(streamId,
									"Channel/Connection closed (@ request)");
							return false;
						}

						long requested =
								(rb[0] & 0xFFL) | ((rb[1] & 0xFFL) << 8) | ((rb[2] & 0xFFL) << 16) | ((rb[3] & 0xFFL) << 24) | ((rb[4] & 0xFFL) << 32) | ((rb[5] & 0xFFL) << 40) | ((rb[6] & 0xFFL) << 48) | ((rb[7] & 0xFFL) << 56);

						onReceive.onRequested(streamId, requested);
					}
					else {
						onReceive.onRequested(streamId, flags);
					}
					break;
				}

				default: {
					if (log.isDebugEnabled()) {
						if (len > 16) {
							byte[] payload = new byte[len - 16];
							int r = readFully(in, payload, len - 16);
							log.debug("onUnknown/%d/len=%d/%d%n",
									streamId,
									payload.length,
									r);
						}
						else {
							log.debug("onUnknown/%d/len=%d/%d%n", streamId, 0, 0);
						}
					}
				}
			}

			return true;
		}
		catch (IOException ex)

		{
			onReceive.onError(-1, "I/O error while reading data: " + ex);
			return false;
		}

	}

	static void onNext(long streamId,
			int flags,
			byte[] payload,
			int count,
			int read,
			StreamOperations endpoint,
			byte[] readBuffer) {
		if (count != read) {
			endpoint.onError(streamId,
					new IOException("Partial value received: expected = " + payload.length + ", actual = " + read));
		}
		else {
			Object o;

			try {
				o = decode(flags, payload, count, readBuffer);
			}
			catch (IOException | ClassNotFoundException ex) {
				endpoint.sendCancel(streamId, ex.toString());
				endpoint.onError(streamId, ex);
				return;
			}

			try {
				endpoint.onNext(streamId, o);
			}
			catch (Throwable ex) {
				endpoint.sendCancel(streamId, ex.toString());
				endpoint.onError(streamId, ex);
			}
		}
	}

	static Object decode(int flags, byte[] payload, int len, byte[] readBuffer)
			throws IOException, ClassNotFoundException {
		if (flags == PAYLOAD_INT) {
			int v =
					(payload[0] & 0xFF) | ((payload[1] & 0xFF) << 8) | ((payload[2] & 0xFF) << 16) | ((payload[3] & 0xFF) << 24);
			return v;
		}
		else if (flags == PAYLOAD_LONG) {
			long v =
					(payload[0] & 0xFFL) | ((payload[1] & 0xFFL) << 8) | ((payload[2] & 0xFFL) << 16) | ((payload[3] & 0xFFL) << 24) | ((payload[4] & 0xFFL) << 32) | ((payload[5] & 0xFFL) << 40) | ((payload[6] & 0xFFL) << 48) | ((payload[7] & 0xFFL) << 56);
			return v;
		}
		else if (flags == PAYLOAD_STRING) {
			return readUtf8(payload, 0, len);
		}
		else if (flags == PAYLOAD_BYTES) {
			if (payload == readBuffer) {
				byte[] r = new byte[len];
				System.arraycopy(payload, 0, r, 0, len);
				return r;
			}
			return payload;
		}

		ByteArrayInputStream bin = new ByteArrayInputStream(payload);
		ObjectInputStream oin = new ObjectInputStream(bin);
		return oin.readObject();
	}

	public static void request(OutputStream out,
			long streamId,
			long requested,
			byte[] wb) {
		if (requested < 0 || requested == Long.MAX_VALUE) {
			send(out, streamId, TYPE_REQUEST, 0, REQUEST_UNBOUNDED, wb);
		}
		else if (requested <= 0xFFFFFF) {
			send(out, streamId, TYPE_REQUEST, (int) requested & 0xFFFFFF, EMPTY, wb);
		}
		else {
			send(out, streamId, TYPE_REQUEST, 0, requested, wb);
		}
	}

	static void send(OutputStream out,
			long streamId,
			int type,
			int flags,
			byte[] payload,
			byte[] wb) {
		try {
			int len = 16 + (payload != null ? payload.length : 0);

			wb[0] = (byte) ((len >> 0) & 0xFF);
			wb[1] = (byte) ((len >> 8) & 0xFF);
			wb[2] = (byte) ((len >> 16) & 0xFF);
			wb[3] = (byte) ((len >> 24) & 0xFF);

			wb[4] = (byte) (type & 0xFF);

			wb[5] = (byte) ((flags >> 0) & 0xFF);
			wb[6] = (byte) ((flags >> 8) & 0xFF);
			wb[7] = (byte) ((flags >> 16) & 0xFF);

			wb[8] = (byte) ((int) (streamId >> 0) & 0xFF);
			wb[9] = (byte) ((int) (streamId >> 8) & 0xFF);
			wb[10] = (byte) ((int) (streamId >> 16) & 0xFF);
			wb[11] = (byte) ((int) (streamId >> 24) & 0xFF);
			wb[12] = (byte) ((int) (streamId >> 32) & 0xFF);
			wb[13] = (byte) ((int) (streamId >> 40) & 0xFF);
			wb[14] = (byte) ((int) (streamId >> 48) & 0xFF);
			wb[15] = (byte) ((int) (streamId >> 56) & 0xFF);

			out.write(wb, 0, 16);

			if (payload != null && payload.length != 0) {
				out.write(payload);
			}
		}
		catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	static void send(OutputStream out,
			long streamId,
			int type,
			int flags,
			long payload,
			byte[] wb) {
		try {
			int len = 24;

			wb[0] = (byte) ((len >> 0) & 0xFF);
			wb[1] = (byte) ((len >> 8) & 0xFF);
			wb[2] = (byte) ((len >> 16) & 0xFF);
			wb[3] = (byte) ((len >> 24) & 0xFF);

			wb[4] = (byte) (type & 0xFF);

			wb[5] = (byte) ((flags >> 0) & 0xFF);
			wb[6] = (byte) ((flags >> 8) & 0xFF);
			wb[7] = (byte) ((flags >> 16) & 0xFF);

			wb[8] = (byte) ((int) (streamId >> 0) & 0xFF);
			wb[9] = (byte) ((int) (streamId >> 8) & 0xFF);
			wb[10] = (byte) ((int) (streamId >> 16) & 0xFF);
			wb[11] = (byte) ((int) (streamId >> 24) & 0xFF);
			wb[12] = (byte) ((int) (streamId >> 32) & 0xFF);
			wb[13] = (byte) ((int) (streamId >> 40) & 0xFF);
			wb[14] = (byte) ((int) (streamId >> 48) & 0xFF);
			wb[15] = (byte) ((int) (streamId >> 56) & 0xFF);

			wb[16] = (byte) ((payload >> 0) & 0xFF);
			wb[17] = (byte) ((payload >> 8) & 0xFF);
			wb[18] = (byte) ((payload >> 16) & 0xFF);
			wb[19] = (byte) ((payload >> 24) & 0xFF);
			wb[20] = (byte) ((payload >> 32) & 0xFF);
			wb[21] = (byte) ((payload >> 40) & 0xFF);
			wb[22] = (byte) ((payload >> 48) & 0xFF);
			wb[23] = (byte) ((payload >> 56) & 0xFF);

			out.write(wb, 0, len);
		}
		catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	static byte[] utf8(String s) {
		if (s == null || s.isEmpty()) {
			return EMPTY;
		}
		return s.getBytes(StandardCharsets.UTF_8);
	}

	private ByteArrayStreamProtocol() {
	}

	static final byte[] EMPTY             = new byte[0];
	static final byte[] REQUEST_UNBOUNDED =
			{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
					(byte) 0xFF, (byte) 0x7F};

}
