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
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Processors;
import reactor.Publishers;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.http.NettyHttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Nexus extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>
		implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> {

	private static final Logger log = LoggerFactory.getLogger(Nexus.class);

	private static final String API_STREAM_URL = "/nexus/stream";
	private static final String EXIT_URL       = "/exit";

	private final HttpServer<Buffer, Buffer> server;
	private final JsonCodec<Event, Event>    jsonCodec;
	private final BaseProcessor<Event, Event> eventStream = Processors.emitter(false);
	private final GraphEvent lastState;
	private final ProcessorGroup         group          = Processors.asyncGroup("nexus", 1024, 1, null, null, false);
	private final Function<Event, Event> lastStateMerge = new LastGraphStateMap();

	private final ReactiveSession<Publisher<Event>> cannons;

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
		this.jsonCodec = new JsonCodec<>(Event.class);

		BaseProcessor<Publisher<Event>, Publisher<Event>> cannons = Processors.emitter();

		Publishers.merge(cannons)
		          .subscribe(eventStream);

		this.cannons = cannons.startSession();

		lastState = new GraphEvent(server.getListenAddress()
		                                 .getHostName(), ReactiveStateUtils.newGraph());
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

	/**
	 *
	 * @return
	 */
	public final ReactiveSession<Object> logCannon() {
		BaseProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		this.cannons.submit(Publishers.map(p, new LogMapper()));
		return p.startSession();
	}

	/**
	 *
	 * @return
	 */
	public final ReactiveSession<Object> metricCannon() {
		BaseProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		this.cannons.submit(Publishers.map(p, new MetricMapper()));
		return p.startSession();
	}

	/**
	 *
	 * @return
	 */
	public final ReactiveSession<Object> streamCannon() {
		BaseProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		this.cannons.submit(Publishers.map(p, new GraphMapper()));
		return p.startSession();
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
		channel.responseHeader("Access-Control-Allow-Origin", "*");

		Publisher<Event> eventStream =
				Publishers.capacity(Publishers.map(this.eventStream.dispatchOn(group), lastStateMerge), 1L);

		Publisher<Void> p;
		if (channel.isWebsocket()) {
			p = Publishers.concat(
					NettyHttpServer.upgradeToWebsocket(channel),
					channel.writeBufferWith(jsonCodec.encode(eventStream))
			);
		}
		else {
			p = channel.writeBufferWith(jsonCodec.encode(eventStream));
		}

		return p;
	}

	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}

	private static class Event {

		private final String hostname;

		public Event(String hostname) {
			this.hostname = hostname;
		}

		public String getNexusHost() {
			return hostname;
		}

		public String getType() {
			return getClass().getSimpleName();
		}
	}

	private final static class GraphEvent extends Event {

		private final ReactiveStateUtils.Graph graph;
		private final boolean                  removed;

		public GraphEvent(String name, ReactiveStateUtils.Graph graph) {
			this(name, graph, false);
		}

		public GraphEvent(String name, ReactiveStateUtils.Graph graph, boolean removed) {
			super(name);
			this.graph = graph;
			this.removed = removed;
		}

		public ReactiveStateUtils.Graph getStreams() {
			return graph;
		}

		public boolean isRemoved() {
			return removed;
		}
	}

	private final static class LogEvent extends Event {

		private final String message;
		private final String level;
		private final long   timestamp = System.currentTimeMillis();

		public LogEvent(String name, String message, String level) {
			super(name);
			this.message = message;
			this.level = level;
		}

		public String getMessage() {
			return message;
		}

		public String getLevel() {
			return level;
		}
	}

	private final static class MetricEvent extends Event {

		public MetricEvent(String hostname) {
			super(hostname);
		}
	}

	private final static class SystemEvent extends Event {

		private final Runtime runtime = Runtime.getRuntime();

		public SystemEvent(String hostname) {
			super(hostname);
		}

		public long getFreeMemory() {
			return runtime.freeMemory(); //bytes
		}

		public long getMaxMemory() {
			return runtime.maxMemory(); //bytes
		}

		public long getUsedMemory() {
			return runtime.totalMemory(); //bytes
		}

		public int getActiveThreads() {
			return Thread.activeCount();
		}
	}

	private class LastGraphStateMap implements Function<Event, Event>, ReactiveState.Named {

		@Override
		public Event apply(Event event) {
			if (GraphEvent.class.equals(event.getClass())) {
				lastState.graph.mergeWith(((GraphEvent) event).graph);
				return lastState;
			}
			return event;
		}

		@Override
		public String getName() {
			return "ScanIfGraphEvent";
		}
	}

	private class LogMapper implements Function<Object, Event> {

		@Override
		@SuppressWarnings("unchecked")
		public Event apply(Object o) {
			String level;
			String message;
			if (Tuple2.class.equals(o.getClass())) {
				level = ((Tuple2<String, String>) o).getT1();
				message = ((Tuple2<String, String>) o).getT2();
			}
			else {
				level = null;
				message = o.toString();
			}
			return new LogEvent(server.getListenAddress()
			                          .getHostName(), message, level);
		}
	}

	private class MetricMapper implements Function<Object, Event> {

		@Override
		public Event apply(Object o) {
			return new MetricEvent(server.getListenAddress()
			                             .getHostName());
		}
	}

	private class GraphMapper implements Function<Object, Event> {

		@Override
		public Event apply(Object o) {
			return new GraphEvent(server.getListenAddress()
			                            .getHostName(), ReactiveStateUtils.scan(o));
		}
	}
}
