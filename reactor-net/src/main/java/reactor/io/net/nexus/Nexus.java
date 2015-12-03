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

package reactor.io.net.nexus;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Publishers;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Nexus extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>
		implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> {

	private static final Logger log = LoggerFactory.getLogger(Nexus.class);

	private static final String API_STREAM_URL = "/nexus/stream";
	private static final String EXIT_URL       = "/exit";

	private final HttpServer<Buffer, Buffer>                                    server;
	private final JsonCodec<ReactiveStateUtils.Graph, ReactiveStateUtils.Graph> jsonCodec;

	public static void main(String... args) throws Exception {
		log.info("Deploying Nexus... ");

		Nexus nexus = ReactiveNet.nexus();

		final CountDownLatch stopped = new CountDownLatch(1);

		nexus.server.get(EXIT_URL, new ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>() {
			@Override
			public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {
				stopped.countDown();
				return Publishers.empty();
			}
		});

		nexus.startAndAwait();

		InetSocketAddress addr = nexus.getServer()
		                              .getListenAddress();
		log.info("Nexus Warped, Void Energy available under http://" + addr.getHostName() + ":" + addr.getPort() +
				API_STREAM_URL);

		stopped.await();
	}

	/**
	 *
	 * @param server
	 * @return
	 */
	public static Nexus create(HttpServer<Buffer, Buffer> server) {

		Nexus nexus = new Nexus(server.getDefaultTimer(), server);
		log.info("Warping Nexus...");

		server.get(API_STREAM_URL, nexus);

		return nexus;
	}

	private Nexus(Timer defaultTimer, HttpServer<Buffer, Buffer> server) {
		super(defaultTimer);
		this.server = server;
		this.jsonCodec = new JsonCodec<>(ReactiveStateUtils.Graph.class);
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void startAndAwait() throws InterruptedException {
		Publishers.toReadQueue(start(null))
		          .take();
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void start() throws InterruptedException {
		start(null);
	}

	@Override
	protected Publisher<Void> doStart(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler) {
		return server.start();
	}

	@Override
	protected Publisher<Void> doShutdown() {
		return server.shutdown();
	}

	@Override
	public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {
		//MOCK
		return channel.responseHeader("Access-Control-Allow-Origin", "*")
		              .writeWith(jsonCodec.encode(Publishers.just(ReactiveStateUtils.scan(channel))));
	}

	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}
}
