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
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.Inbound;
import reactor.ipc.Outbound;
import reactor.ipc.connector.StreamRemote;

/**
 * @author Stephane Maldini
 */
final class SimpleConnection implements Inbound<byte[]>, Outbound<byte[]>, StreamRemote {

	final Socket socket;

	final AtomicBoolean terminateOnce;

	final Flux<byte[]> receiver;

	final byte[] readBuffer;

	final byte[] writeBuffer;

	final Scheduler readScheduler;
	final Scheduler writeScheduler;
	final Scheduler dispatcher;

	final InputStream in;

	final OutputStream out;

	SimpleConnection(Socket socket) {
		this(socket, false);
	}

	SimpleConnection(Socket socket, boolean server) {
		this.socket = socket;
		this.terminateOnce = new AtomicBoolean();
		this.readBuffer = new byte[16];
		this.writeBuffer = new byte[32];

		this.dispatcher =
				Schedulers.newParallel("simple-" + (server ? "server" : "client") + "-io",
						2,
						true);
		
		this.readScheduler = Schedulers.single(dispatcher);
		this.writeScheduler = Schedulers.single(dispatcher);

		InputStream in;
		OutputStream out;

		try {
			in = socket.getInputStream();
			out = socket.getOutputStream();
		}
		catch (IOException io) {
			throw Exceptions.propagate(io);
		}

		this.in = in;
		this.out = out;

		this.receiver = Flux.<byte[]>generate(sink -> {
			if (readFully(in, readBuffer, 16) < 16) {
				sink.complete();
			}
			else {
				sink.next(readBuffer);
			}
		}).subscribeOn(readScheduler)
		  .share();


	}

	void tryClose() {
		try {
			readScheduler.shutdown();
			writeScheduler.shutdown();
			dispatcher.shutdown();
			in.close();
			out.close();
		}
		catch (IOException io) {
			//IGNORE
		}
	}

	@Override
	public Flux<byte[]> receive() {
		return receiver;
	}

	@Override
	public Socket delegate() {
		return socket;
	}

	@Override
	public Mono<Void> send(Publisher<? extends byte[]> dataStream) {
		throw new UnsupportedOperationException();
	}

	byte[] encode(Object o, IntConsumer flagOut) throws IOException {

		if (o instanceof Integer) {
			flagOut.accept(ByteArrayStreamProtocol.PAYLOAD_INT);
			byte[] r = new byte[4];
			int v = (Integer) o;
			r[0] = (byte) (v & 0xFF);
			r[1] = (byte) ((v >> 8) & 0xFF);
			r[2] = (byte) ((v >> 16) & 0xFF);
			r[3] = (byte) ((v >> 24) & 0xFF);
			return r;
		}
		else if (o instanceof Long) {
			flagOut.accept(ByteArrayStreamProtocol.PAYLOAD_LONG);
			byte[] r = new byte[8];
			long v = (Long) o;
			r[0] = (byte) (v & 0xFF);
			r[1] = (byte) ((v >> 8) & 0xFF);
			r[2] = (byte) ((v >> 16) & 0xFF);
			r[3] = (byte) ((v >> 24) & 0xFF);
			r[4] = (byte) ((v >> 32) & 0xFF);
			r[5] = (byte) ((v >> 40) & 0xFF);
			r[6] = (byte) ((v >> 48) & 0xFF);
			r[7] = (byte) ((v >> 56) & 0xFF);
			return r;
		}
		else if (o instanceof String) {
			flagOut.accept(ByteArrayStreamProtocol.PAYLOAD_STRING);
			return ByteArrayStreamProtocol.utf8((String) o);
		}
		else if (o instanceof byte[]) {
			flagOut.accept(ByteArrayStreamProtocol.PAYLOAD_BYTES);
			return ((byte[]) o).clone();
		}

		flagOut.accept(ByteArrayStreamProtocol.PAYLOAD_OBJECT);

		ByteArrayOutputStream bout = new ByteArrayOutputStream();

		try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
			oout.writeObject(o);
		}

		return bout.toByteArray();
	}

	static void flush(OutputStream out) {
		try {
			out.flush();
		}
		catch (IOException ex) {
			Operators.onErrorDropped(ex);
		}
	}

	@Override
	public void sendNew(long streamId, String function) {
		writeScheduler.schedule(() -> {
			ByteArrayStreamProtocol.open(out, streamId, function, writeBuffer);
			flush(out);
		});
	}

	@Override
	public void sendNext(long streamId, Object o) throws IOException {

		OnNextTask task = new OnNextTask(streamId, out, writeBuffer);

		task.payload = encode(o, task);

		writeScheduler.schedule(task);
	}

	static final class OnNextTask implements Runnable, IntConsumer {

		final long         streamId;
		final OutputStream out;
		final byte[]       writeBuffer;
		byte[] payload;
		int    flags;

		public OnNextTask(long streamId, OutputStream out, byte[] writeBuffer) {
			this.streamId = streamId;
			this.out = out;
			this.writeBuffer = writeBuffer;
		}

		@Override
		public void run() {
			ByteArrayStreamProtocol.next(out, streamId, flags, payload, writeBuffer);
			flush(out);
		}

		@Override
		public void accept(int value) {
			this.flags = value;
		}
	}

	@Override
	public void sendError(long streamId, Throwable e) {
		writeScheduler.schedule(() -> {
			ByteArrayStreamProtocol.error(out, streamId, e, writeBuffer);
			flush(out);
		});
	}

	@Override
	public void sendComplete(long streamId) {
		writeScheduler.schedule(() -> {
			ByteArrayStreamProtocol.complete(out, streamId, writeBuffer);
			flush(out);
		});
	}

	@Override
	public void sendCancel(long streamId, String reason) {
		writeScheduler.schedule(() -> {
			ByteArrayStreamProtocol.cancel(out, streamId, reason, writeBuffer);
			flush(out);
		});
	}

	@Override
	public void sendRequested(long streamId, long requested) {
		writeScheduler.schedule(() -> {
			ByteArrayStreamProtocol.request(out, streamId, requested, writeBuffer);
			flush(out);
		});
	}

	@Override
	public boolean isClosed() {
		return terminateOnce.get();
	}

	static int readFully(InputStream in, byte[] output, int count) {
		int offset = 0;
		int remaining = count;

		try {
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
		}
		catch (IOException io) {
			throw Exceptions.propagate(io);
		}
		return offset;
	}

	Object decode(int flags, byte[] payload, int len)
			throws IOException, ClassNotFoundException {
		if (flags == ByteArrayStreamProtocol.PAYLOAD_INT) {
			int v =
					(payload[0] & 0xFF) | ((payload[1] & 0xFF) << 8) | ((payload[2] & 0xFF) << 16) | ((payload[3] & 0xFF) << 24);
			return v;
		}
		else if (flags == ByteArrayStreamProtocol.PAYLOAD_LONG) {
			long v =
					(payload[0] & 0xFFL) | ((payload[1] & 0xFFL) << 8) | ((payload[2] & 0xFFL) << 16) | ((payload[3] & 0xFFL) << 24) | ((payload[4] & 0xFFL) << 32) | ((payload[5] & 0xFFL) << 40) | ((payload[6] & 0xFFL) << 48) | ((payload[7] & 0xFFL) << 56);
			return v;
		}
		else if (flags == ByteArrayStreamProtocol.PAYLOAD_STRING) {
			return ByteArrayStreamProtocol.readUtf8(payload, 0, len);
		}
		else if (flags == ByteArrayStreamProtocol.PAYLOAD_BYTES) {
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

	@Override
	public Scheduler inboundScheduler() {
		return readScheduler;
	}
}
