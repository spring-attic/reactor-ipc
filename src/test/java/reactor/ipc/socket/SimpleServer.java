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
import reactor.core.Cancellation;
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
	public Mono<Void> newHandler(BiFunction<? super Inbound<byte[]>, ? super Outbound<byte[]>, ? extends Publisher<Void>> channelHandler) {

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

			Cancellation c = acceptor.schedule(() -> socketAccept(ssocket,
					sink,
					channelHandler,
					done));

			sink.setCancellation(() -> {
				if (done.compareAndSet(false, true)) {
					acceptor.shutdown();
					c.dispose();
					try {
						ssocket.close();
					}
					catch (IOException ex) {
						sink.error(ex);
					}
				}
			});
		});
	}

	@Override
	public Scheduler scheduler() {
		return scheduler;
	}

	void socketAccept(ServerSocket ssocket,
			MonoSink<Void> sink,
			BiFunction<? super Inbound<byte[]>, ? super Outbound<byte[]>, ? extends Publisher<Void>> channelHandler,
			AtomicBoolean done) {
		while (!Thread.currentThread()
		              .isInterrupted()) {
			Socket socket;

			try {
				socket = ssocket.accept();

			}
			catch (IOException e) {
				if (!done.get()) {
					sink.error(e);
				}
				return;
			}

			try {
				SimpleConnection connection = new SimpleConnection(socket, true);
				Publisher<Void> closing = channelHandler.apply(connection, connection);
				Flux.from(closing)
				    .subscribe(null, t -> tryClose(socket), () -> tryClose(socket));
			}
			catch (Throwable ex) {
				tryClose(socket);
			}
		}
	}

	void tryClose(Socket socket) {
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
}
