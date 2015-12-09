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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Processors;
import reactor.Publishers;
import reactor.Timers;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Consumer;
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
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.http.NettyHttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Nexus extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>
		implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>, ReactiveState.FeedbackLoop {

	private static final Logger log = LoggerFactory.getLogger(Nexus.class);

	private static final String API_STREAM_URL = "/nexus/stream";
	private final HttpServer<Buffer, Buffer> server;
	private final JsonCodec<Event, Event>    jsonCodec;
	private final BaseProcessor<Event, Event> eventStream = Processors.emitter(false);
	private final GraphEvent  lastState;
	private final SystemEvent lastSystemState;
	private final ProcessorGroup         group          = Processors.asyncGroup("nexus", 1024, 1, null, null, false);
	private final Function<Event, Event> lastStateMerge = new LastGraphStateMap();
	private final Timer                  timer          = Timers.create("nexus-poller");

	@SuppressWarnings("unused")
	private volatile FederatedClient[] federatedClients;

	static final AtomicReferenceFieldUpdater<Nexus, FederatedClient[]> FEDERATED =
			PlatformDependent.newAtomicReferenceFieldUpdater(Nexus.class, "federatedClients");

	private final ReactiveSession<Publisher<Event>> cannons;

	private boolean systemStats;
	private long    systemStatsPeriod;

	public static void main(String... args) throws Exception {
		log.info("Deploying Nexus... ");

		Nexus nexus = ReactiveNet.nexus();

		final CountDownLatch stopped = new CountDownLatch(1);

		nexus.startAndAwait();

		log.info("CTRL-C to return...");
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
		                                 .toString(), ReactiveStateUtils.newGraph());

		lastSystemState = new SystemEvent(server.getListenAddress()
		                                        .toString());
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void startAndAwait() throws InterruptedException {
		Publishers.toReadQueue(start(null))
		          .take();
		InetSocketAddress addr = server.getListenAddress();
		log.info("Nexus Warped. Transmitting signal to troops under http://" + addr.getHostName() + ":" + addr.getPort() +
				API_STREAM_URL);
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final void start() throws InterruptedException {
		start(null);
	}

	public final Nexus disableSystemStats() {
		this.systemStats = false;
		this.systemStatsPeriod = -1;
		return this;
	}

	public final Nexus withSystemStats() {
		return withSystemStats(true, 1);
	}

	public final Nexus withSystemStats(boolean enabled, long period) {
		return withSystemStats(enabled, period, TimeUnit.SECONDS);
	}

	public final Nexus withSystemStats(boolean enabled, long period, TimeUnit unit) {
		this.systemStatsPeriod = unit == null || period < 1L ? 1000 : TimeUnit.MILLISECONDS.convert(period, unit);
		this.systemStats = enabled;
		return this;
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

	/**
	 *
	 * @param o
	 * @param <E>
	 * @return
	 */
	public final <E> E monitor(E o) {
		return monitor(o, -1L);
	}

	/**
	 *
	 * @param o
	 * @param period
	 * @param <E>
	 * @return
	 */
	public final <E> E monitor(E o, long period) {
		return monitor(o, period, null);
	}

	/**
	 *
	 * @param o
	 * @param period
	 * @param unit
	 * @param <E>
	 * @return
	 */
	public final <E> E monitor(final E o, long period, TimeUnit unit) {

		final long _period = period > 0 ? (unit != null ? TimeUnit.MILLISECONDS.convert(period, unit) : period) : 400L;

		BaseProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		final ReactiveSession<Object> session = p.startSession();
		timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				session.submit(ReactiveStateUtils.scan(o));
			}
		}, _period, TimeUnit.MILLISECONDS);

		this.cannons.submit(Publishers.map(p, new GraphMapper()));

		return o;
	}

	/**
	 *
	 * @param urls
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Nexus federate(String... urls) {
		if (urls == null || urls.length == 0) {
			return this;
		}

		for (; ; ) {
			FederatedClient[] clients = federatedClients;

			int n;
			if (clients != null) {
				n = clients.length;
			}
			else {
				n = 0;
			}
			FederatedClient[] newClients = new FederatedClient[n + urls.length];

			if (n > 0) {
				System.arraycopy(clients, 0, newClients, 0, n);
			}

			for (int i = n; i < newClients.length; i++) {
				newClients[i] = new FederatedClient(urls[i - n]);
			}

			if (FEDERATED.compareAndSet(this, clients, newClients)) {
				break;
			}
		}

		return this;
	}

	@Override
	public Publisher<Void> apply(HttpChannel<Buffer, Buffer> channel) {
		channel.responseHeader("Access-Control-Allow-Origin", "*");

		Publisher<Event> eventStream = Publishers.map(this.eventStream.dispatchOn(group), lastStateMerge);

		Publisher<Void> p;
		if (channel.isWebsocket()) {
			p = Publishers.concat(NettyHttpServer.upgradeToWebsocket(channel),
					channel.writeBufferWith(federateAndEncode(channel, eventStream)));
		}
		else {
			p = channel.writeBufferWith(federateAndEncode(channel, eventStream));
		}

		return p;
	}

	/**
	 *
	 * @return
	 */
	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}

	@Override
	protected Publisher<Void> doStart(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler) {
		if (systemStats) {
			BaseProcessor<Event, Event> p = ProcessorGroup.<Event>sync().dispatchOn();
			this.cannons.submit(p);
			final ReactiveSession<Event> session = p.startSession();

			timer.schedule(new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					session.submit(lastSystemState.scan());
				}
			}, systemStatsPeriod, TimeUnit.MILLISECONDS);
		}
		return server.start();
	}

	@Override
	protected Publisher<Void> doShutdown() {
		timer.cancel();
		return server.shutdown();
	}

	@Override
	public Object delegateInput() {
		return eventStream;
	}

	@Override
	public Object delegateOutput() {
		return server;
	}

	private Publisher<? extends Buffer> federateAndEncode(HttpChannel<Buffer, Buffer> c, Publisher<Event> stream) {
		FederatedClient[] clients = federatedClients;
		if (clients == null || clients.length == 0) {
			return Publishers.capacity(jsonCodec.encode(stream), 1L);
		}

		Publisher<Buffer> mergedUpstreams =
				Publishers.merge(Publishers.map(Publishers.from(Arrays.asList(clients)), new FederatedMerger(c)));

		return Publishers.capacity(Publishers.merge(jsonCodec.encode(stream), mergedUpstreams), 1L);
	}

	private static class Event {

		private final String nexusHost;

		public Event(String nexusHost) {
			this.nexusHost = nexusHost;
		}

		public String getNexusHost() {
			return nexusHost;
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
		private final long timestamp = System.currentTimeMillis();

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

		private static final Runtime  runtime  = Runtime.getRuntime();
		private static final JvmStats jvmStats = new JvmStats();

		private final Map<Thread, ThreadState> threads = new WeakHashMap<>();

		public SystemEvent(String hostname) {
			super(hostname);
		}

		public Collection<ThreadState> getThreads() {
			return threads.values();
		}

		public JvmStats getJvmStats() {
			return jvmStats;
		}

		private SystemEvent scan() {
			int active = Thread.activeCount();
			Thread[] currentThreads = new Thread[active];
			int n = Thread.enumerate(currentThreads);

			for (int i = 0; i < n; i++) {
				if (!threads.containsKey(currentThreads[i])) {
					threads.put(currentThreads[i], new ThreadState(currentThreads[i]));
				}
			}
			return this;
		}

		final static class JvmStats {

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

			public int getAvailableProcessors(){
				return runtime.availableProcessors();
			}
		}

		final static class ThreadState {

			private transient final Thread thread;

			public ThreadState(Thread thread) {
				this.thread = thread;
			}

			public String getName() {
				return thread.getName();
			}

			public boolean isAlive() {
				return thread.isAlive();
			}

			public boolean isInterrupted() {
				return thread.isInterrupted();
			}

			public long getContextHash() {
				return thread.getContextClassLoader()
				             .hashCode();
			}

			public long getId() {
				return thread.getId();
			}

			public Thread.State getState() {
				return thread.getState();
			}

			public String getThreadGroup() {
				ThreadGroup group = thread.getThreadGroup();
				return group != null ? thread.getThreadGroup()
				             .getName() : null;
			}

			public boolean isDaemon() {
				return thread.isDaemon();
			}

			public int getPriority() {
				return thread.getPriority();
			}
		}
	}

	private class LastGraphStateMap implements Function<Event, Event>, ReactiveState.Named {

		@Override
		public Event apply(Event event) {
			if (GraphEvent.class.equals(event.getClass())) {
				lastState.graph.mergeWith(((GraphEvent) event).graph);
				//lastState.graph.removeTerminatedNodes();
				return lastState;
			}
			return event;
		}

		@Override
		public String getName() {
			return "ScanIfGraphEvent";
		}
	}

	private final class LogMapper implements Function<Object, Event> {

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
			                          .toString(), message, level);
		}
	}

	private static final class FederatedMerger implements Function<FederatedClient, Publisher<Buffer>> {

		private final HttpChannel<Buffer, Buffer> c;

		public FederatedMerger(HttpChannel<Buffer, Buffer> c) {
			this.c = c;
		}

		@Override
		public Publisher<Buffer> apply(FederatedClient o) {
			return Publishers.flatMap(o.client.ws(o.targetAPI),
					new Function<HttpChannel<Buffer, Buffer>, Publisher<Buffer>>() {
						@Override
						public Publisher<Buffer> apply(HttpChannel<Buffer, Buffer> channel) {
							return channel.input();
						}
					});
		}
	}

	private final class MetricMapper implements Function<Object, Event> {

		@Override
		public Event apply(Object o) {
			return new MetricEvent(server.getListenAddress()
			                             .toString());
		}
	}

	private final class GraphMapper implements Function<Object, Event> {

		@Override
		public Event apply(Object o) {
			return new GraphEvent(server.getListenAddress()
			                            .toString(),
					ReactiveStateUtils.Graph.class.equals(o.getClass()) ? ((ReactiveStateUtils.Graph) o) :
							ReactiveStateUtils.scan(o));
		}
	}

	private final class FederatedClient {

		private final HttpClient<Buffer, Buffer> client;
		private final String                     targetAPI;

		public FederatedClient(String targetAPI) {
			this.targetAPI = targetAPI;
			this.client = ReactiveNet.httpClient();
		}
	}
}
