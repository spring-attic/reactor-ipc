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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.connector.Inbound;
import reactor.ipc.connector.Outbound;

/**
 * @author Stephane Maldini
 */
public final class SimpleServer extends SimplePeer  {

	static public SimpleServer create(int port) {
		return new SimpleServer(port, null);
	}

	static public SimpleServer create(int port, InetAddress bindAddress) {
		Objects.requireNonNull(bindAddress, "bindAddress");
		return new SimpleServer(port, bindAddress);
	}

	final int         port;
	final InetAddress address;

	SimpleServer(int port, InetAddress address) {
		this.port = port;
		this.address = address;
	}

	@Override
	public Mono<? extends Disposable> newHandler(BiFunction<? super Inbound<byte[]>, ? super Outbound<byte[]>, ? extends Publisher<Void>> ioHandler) {

		return Mono.create(sink -> {
			ServerSocket ssocket;
			Scheduler acceptor =
					Schedulers.newParallel("simple-server-connection", 1, true);

			try {
				if (address == null) {
					ssocket = new ServerSocket(port);
				}
				else {
					ssocket = new ServerSocket(port, 50, address);
				}
			}
			catch (IOException e) {
				sink.error(e);
				return;
			}

			AtomicBoolean done = new AtomicBoolean();
			ServerListening connectedState =
					new ServerListening(ssocket, done, sink, acceptor);
			Disposable c =
					acceptor.schedule(() -> socketAccept(ioHandler, connectedState));

			sink.setCancellation(() -> connectedState.close(c));
		});
	}

	static void socketAccept(
			BiFunction<? super Inbound<byte[]>, ? super Outbound<byte[]>, ? extends Publisher<Void>> ioHandler,
			ServerListening connectedState) {

		connectedState.sink.success(connectedState);

			while (!Thread.currentThread()
			              .isInterrupted()) {
				Socket socket;

				try {
					socket = connectedState.ssocket.accept();

				}
				catch (IOException e) {
					if (!connectedState.done.get()) {
						connectedState.sink.error(e);
					}
					return;
				}

				try {
					SimpleConnection connection = new SimpleConnection(socket, true);
					Publisher<Void> closing = ioHandler.apply(connection, connection);
					Flux.from(closing)
					    .subscribe(null, connection::closeError, connection::close);
				}
				catch (Throwable ex) {
					tryClose(socket);
				}

			}
	}

	static void tryClose(Socket socket) {
		try {
			socket.close();
		}
		catch (IOException e) {
			//IGNORE
		}
	}

	static final Scheduler scheduler =
			Schedulers.fromExecutorService(Executors.newCachedThreadPool(r -> {
				Thread t = new Thread(r, "test-server-pool");
				t.setDaemon(true);
				return t;
			}));

	static final class ServerListening implements Disposable {

		final ServerSocket           ssocket;
		final AtomicBoolean          done;
		final MonoSink<Disposable> sink;
		final Scheduler              acceptor;

		public ServerListening(ServerSocket ssocket,
				AtomicBoolean done,
				MonoSink<Disposable> sink,
				Scheduler acceptor) {
			this.ssocket = ssocket;
			this.done = done;
			this.sink = sink;
			this.acceptor = acceptor;
		}

		@Override
		public void dispose() {
			close(null);
		}

		void close(Disposable c) {
			if (done.compareAndSet(false, true)) {
				acceptor.shutdown();
				if (c != null) {
					c.dispose();
				}
				try {
					ssocket.close();
				}
				catch (IOException ex) {
					return;
				}
			}
		}
	}
}
