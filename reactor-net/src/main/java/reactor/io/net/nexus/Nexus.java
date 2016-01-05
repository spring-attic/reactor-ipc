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

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import reactor.Flux;
import reactor.Mono;
import reactor.Processors;
import reactor.Subscribers;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.FluxProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.Logger;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.WaitStrategy;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.timer.Timer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveNet;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.http.NettyHttpServer;

import static reactor.core.support.ReactiveStateUtils.property;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Nexus extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>
		implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>, ReactiveState.FeedbackLoop {

	private static final Logger log            = Logger.getLogger(Nexus.class);
	private static final String API_STREAM_URL = "/nexus/stream";

	private final HttpServer<Buffer, Buffer>        server;
	private final GraphEvent                        lastState;
	private final SystemEvent                       lastSystemState;
	private final FluxProcessor<Event, Event>       eventStream;
	private final ProcessorGroup                    group;
	private final Function<Event, Event>            lastStateMerge;
	private final Timer                             timer;
	private final ReactiveSession<Publisher<Event>> cannons;

	@SuppressWarnings("unused")
	private volatile FederatedClient[] federatedClients;

	static final AtomicReferenceFieldUpdater<Nexus, FederatedClient[]> FEDERATED =
			PlatformDependent.newAtomicReferenceFieldUpdater(Nexus.class, "federatedClients");

	private long                 systemStatsPeriod;
	private boolean              systemStats;
	private boolean              logExtensionEnabled;
	private NexusLoggerExtension logExtension;

	private long websocketCapacity = 1L;

	public static void main(String... args) throws Exception {
		log.info("Deploying Nexus... ");

		Nexus nexus = ReactiveNet.nexus();

		final CountDownLatch stopped = new CountDownLatch(1);

		nexus.startAndAwait();

		log.info("CTRL-C to return...");
		stopped.await();
	}

	/**
	 * @param server
	 *
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
		this.eventStream = Processors.emitter(false);
		this.lastStateMerge = new LastGraphStateMap();
		this.timer = Timers.create("nexus-poller");
		this.group = Processors.asyncGroup("nexus", 1024, 1, null, null, false, new Supplier<WaitStrategy>() {
			@Override
			public WaitStrategy get() {
				return new WaitStrategy.Blocking();
			}
		});

		FluxProcessor<Publisher<Event>, Publisher<Event>> cannons = Processors.emitter();

		Flux.merge(cannons)
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
		start().get();
		InetSocketAddress addr = server.getListenAddress();
		log.info("Nexus Warped. Transmitting signal to troops under http://" + addr.getHostName() + ":" + addr.getPort() +
				API_STREAM_URL);
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 */
	public final Mono<Void> start() throws InterruptedException {
		return start(null);
	}

	/**
	 * @return
	 */
	public final Nexus capacity(long capacity) {
		this.websocketCapacity = capacity;
		return this;
	}

	/**
	 * @return
	 */
	public final Nexus disableLogTail() {
		this.logExtensionEnabled = false;
		return this;
	}

	/**
	 * @return
	 */
	public final Nexus withLogTail() {
		this.logExtensionEnabled = true;
		return this;
	}

	/**
	 * @return
	 */
	public final Nexus withSystemStats() {
		return withSystemStats(true, 1);
	}

	/**
	 * @param enabled
	 * @param period
	 *
	 * @return
	 */
	public final Nexus withSystemStats(boolean enabled, long period) {
		return withSystemStats(enabled, period, TimeUnit.SECONDS);
	}

	/**
	 * @param enabled
	 * @param period
	 * @param unit
	 *
	 * @return
	 */
	public final Nexus withSystemStats(boolean enabled, long period, TimeUnit unit) {
		this.systemStatsPeriod = unit == null || period < 1L ? 1000 : TimeUnit.MILLISECONDS.convert(period, unit);
		this.systemStats = enabled;
		return this;
	}

	/**
	 * @return
	 */
	public final ReactiveSession<Object> metricCannon() {
		FluxProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		this.cannons.submit(p.map(new MetricMapper()));
		return p.startSession();
	}

	/**
	 * @return
	 */
	public final ReactiveSession<Object> streamCannon() {
		FluxProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		this.cannons.submit(p.map(new GraphMapper()));
		return p.startSession();
	}

	/**
	 * @param o
	 * @param <E>
	 *
	 * @return
	 */
	public final <E> E monitor(E o) {
		return monitor(o, -1L);
	}

	/**
	 * @param o
	 * @param period
	 * @param <E>
	 *
	 * @return
	 */
	public final <E> E monitor(E o, long period) {
		return monitor(o, period, null);
	}

	/**
	 * @param o
	 * @param period
	 * @param unit
	 * @param <E>
	 *
	 * @return
	 */
	public final <E> E monitor(final E o, long period, TimeUnit unit) {

		final long _period = period > 0 ? (unit != null ? TimeUnit.MILLISECONDS.convert(period, unit) : period) : 400L;

		FluxProcessor<Object, Object> p = ProcessorGroup.sync()
		                                                .dispatchOn();
		final ReactiveSession<Object> session = p.startSession();
		log.info("State Monitoring Starting on " + ReactiveStateUtils.getName(o));
		timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				if (!session.isCancelled()) {
					session.emit(ReactiveStateUtils.scan(o));
				}
				else {
					log.info("State Monitoring stopping on " + ReactiveStateUtils.getName(o));
					throw CancelException.INSTANCE;
				}
			}
		}, _period, TimeUnit.MILLISECONDS);

		this.cannons.submit(p.map(new GraphMapper()));

		return o;
	}

	/**
	 * @param urls
	 *
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

		Flux<Event> eventStream = this.eventStream.dispatchOn(group)
		                                          .map(lastStateMerge);

		Publisher<Void> p;
		if (channel.isWebsocket()) {
			p = Flux.concat(NettyHttpServer.upgradeToWebsocket(channel),
					channel.writeBufferWith(federateAndEncode(channel, eventStream)));
		}
		else {
			p = channel.writeBufferWith(federateAndEncode(channel, eventStream));
		}

		channel.input()
		       .subscribe(Subscribers.consumer(new Consumer<Buffer>() {
			       @Override
			       public void accept(Buffer buffer) {
				       String command = buffer.asString();
				       int indexArg = command.indexOf("\n");
				       if (indexArg > 0) {
					       String action = command.substring(0, indexArg);
					       String arg = command.length() > indexArg ? command.substring(indexArg + 1) : null;
					       log.info("Received " + "[" + action + "]" + " " + "[" + arg + ']');
//					if(action.equals("pause") && !arg.isEmpty()){
//						((EmitterProcessor)Nexus.this.eventStream).pause();
//					}
//					else if(action.equals("resume") && !arg.isEmpty()){
//						((EmitterProcessor)Nexus.this.eventStream).resume();
//					}
				       }
			       }
		       }));

		return p;
	}

	/**
	 * @return
	 */
	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}

	@Override
	protected Mono<Void> doStart(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler) {

		if (logExtensionEnabled) {
			FluxProcessor<Event, Event> p =
					RingBufferProcessor.share("nexus-log-sink", 256, new WaitStrategy.Blocking());
			cannons.submit(p);
			logExtension = new NexusLoggerExtension(server.getListenAddress()
			                                              .toString(), p.startSession());

			//monitor(p);
			if (!Logger.enableExtension(logExtension)) {
				log.warn("Couldn't setup logger extension as one is already in place");
				logExtension = null;
			}
		}

		if (systemStats) {
			FluxProcessor<Event, Event> p = ProcessorGroup.<Event>sync().dispatchOn();
			this.cannons.submit(p);
			final ReactiveSession<Event> session = p.startSession();
			log.info("System Monitoring Starting");
			timer.schedule(new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					if (!session.isCancelled()) {
						session.submit(lastSystemState.scan());
					}
					else {
						log.info("System Monitoring Stopped");
						throw CancelException.INSTANCE;
					}
				}
			}, systemStatsPeriod, TimeUnit.MILLISECONDS);
		}

		return server.start();
	}

	@Override
	protected Mono<Void> doShutdown() {
		timer.cancel();
		this.cannons.finish();
		this.eventStream.onComplete();
		if (logExtension != null) {
			Logger.disableExtension(logExtension);
			logExtension.logSink.finish();
			logExtension = null;
		}
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

	private Flux<? extends Buffer> federateAndEncode(HttpChannel<Buffer, Buffer> c, Flux<Event> stream) {
		FederatedClient[] clients = federatedClients;
		if (clients == null || clients.length == 0) {
			return stream.map(BUFFER_STRING_FUNCTION)
			             .capacity(websocketCapacity);
		}
		Flux<Buffer> mergedUpstreams = Flux.merge(Flux.fromArray(clients)
		                                              .map(new FederatedMerger(c)));

		return Flux.merge(stream.map(BUFFER_STRING_FUNCTION), mergedUpstreams)
		           .capacity(websocketCapacity);
	}

	private static final Function<Event, Buffer> BUFFER_STRING_FUNCTION = new StringToBuffer();

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

		public GraphEvent(String name, ReactiveStateUtils.Graph graph) {
			super(name);
			this.graph = graph;
		}

		public ReactiveStateUtils.Graph getStreams() {
			return graph;
		}

		@Override
		public String toString() {
			return "{ " + property("streams", getStreams()) +
					", " + property("type", getType()) +
					", " + property("timestamp", System.currentTimeMillis()) +
					", " + property("nexusHost", getNexusHost()) + " }";
		}
	}

	private final static class RemovedGraphEvent extends Event {

		private final Collection<String> ids;

		public RemovedGraphEvent(String name, Collection<String> ids) {
			super(name);
			this.ids = ids;
		}

		public Collection<String> getStreams() {
			return ids;
		}

		@Override
		public String toString() {
			return "{ " + property("streams", getStreams()) +
					", " + property("type", getType()) +
					", " + property("timestamp", System.currentTimeMillis()) +
					", " + property("nexusHost", getNexusHost()) + " }";
		}
	}

	private final static class LogEvent extends Event {

		private final String message;
		private final String category;
		private final Level  level;
		private final long   threadId;
		private final String origin;
		private final String data;
		private final String kind;
		private final long timestamp = System.currentTimeMillis();

		public LogEvent(String name, String category, Level level, String message, Object... args) {
			super(name);
			this.threadId = Thread.currentThread()
			                      .getId();
			this.message = message;
			this.level = level;
			this.category = category;
			if (args != null && args.length == 3) {
				this.kind = args[0].toString();
				this.data = args[1] != null ? args[1].toString() : null;
				this.origin = ReactiveStateUtils.getIdOrDefault(args[2]);
			}
			else {
				this.origin = null;
				this.kind = null;
				this.data = null;
			}
		}

		public String getMessage() {
			return message;
		}

		public String getCategory() {
			return category;
		}

		public String getOrigin() {
			return origin;
		}

		public String getKind() {
			return kind;
		}

		public String getData() {
			return data;
		}

		public Level getLevel() {
			return level;
		}

		public long getThreadId() {
			return threadId;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "{ " + property("timestamp", getTimestamp()) +
					", " + property("level", getLevel().getName()) +
					", " + property("category", getCategory()) +
					(kind != null ? ", " + property("kind", getKind()) : "") +
					(origin != null ? ", " + property("origin", getOrigin()) : "") +
					(data != null ? ", " + property("data", getData()) : "") +
					", " + property("message", getMessage()) +
					", " + property("threadId", getThreadId()) +
					", " + property("type", getType()) +
					", " + property("nexusHost", getNexusHost()) + " }";
		}
	}

	private final static class MetricEvent extends Event {

		public MetricEvent(String hostname) {
			super(hostname);
		}

		@Override
		public String toString() {
			return "{ " + property("nexusHost", getNexusHost()) +
					", " + property("type", getType()) +
					", " + property("timestamp", System.currentTimeMillis()) +
					" }";
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

		@Override
		public String toString() {
			return "{ " + property("jvmStats", getJvmStats()) +
					", " + property("threads", getThreads()) +
					", " + property("type", getType()) +
					", " + property("timestamp", System.currentTimeMillis()) +
					", " + property("nexusHost", getNexusHost()) + " }";
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

			public int getAvailableProcessors() {
				return runtime.availableProcessors();
			}

			@Override
			public String toString() {
				return "{ " + property("freeMemory", getFreeMemory()) +
						", " + property("maxMemory", getMaxMemory()) +
						", " + property("usedMemory", getUsedMemory()) +
						", " + property("activeThreads", getActiveThreads()) +
						", " + property("availableProcessors", getAvailableProcessors()) + " }";
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
				if (thread.getContextClassLoader() != null) {
					return thread.getContextClassLoader()
					             .hashCode();
				}
				else {
					return -1;
				}
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

			@Override
			public String toString() {
				return "{ " + property("id", getId()) +
						", " + property("name", getName()) +
						", " + property("alive", isAlive()) +
						", " + property("state", getState().name()) +
						(getThreadGroup() != null ? ", " + property("threadGroup", getThreadGroup()) : "") +
						(getContextHash() != -1 ? ", " + property("contextHash", getContextHash()) : "") +
						", " + property("interrupted", isInterrupted()) +
						", " + property("priority", getPriority()) +
						", " + property("daemon", isDaemon()) + " }";
			}
		}
	}

	private static class StringToBuffer implements Function<Event, Buffer> {

		@Override
		public Buffer apply(Event event) {
			try {
				return reactor.io.buffer.StringBuffer.wrap(event.toString()
				                                                .getBytes("UTF-8"));
			}
			catch (UnsupportedEncodingException e) {
				throw ReactorFatalException.create(e);
			}
		}
	}

	private final static class NexusLoggerExtension implements Logger.Extension {

		final ReactiveSession<Event> logSink;
		final String                 hostname;

		public NexusLoggerExtension(String hostname, ReactiveSession<Event> logSink) {
			this.logSink = logSink;
			this.hostname = hostname;
		}

		@Override
		public void log(String category, Level level, String msg, Object... arguments) {
			String computed = Logger.format(msg, arguments);
			ReactiveSession.Emission emission;

			if (arguments != null && arguments.length == 3 && ReactiveStateUtils.isLogging(arguments[2])) {
				if (!(emission = logSink.emit(new LogEvent(hostname, category, level, computed, arguments))).isOk()) {
					//System.out.println(emission+ " "+computed);
				}
			}
			else {
				if (!(emission = logSink.emit(new LogEvent(hostname, category, level, computed))).isOk()) {
					//System.out.println(emission+ " "+computed);
				}
			}
		}
	}

	private class LastGraphStateMap implements Function<Event, Event>, ReactiveState.Named {

		@Override
		public Event apply(Event event) {
			if (GraphEvent.class.equals(event.getClass())) {
				lastState.graph.mergeWith(((GraphEvent) event).graph);
//				Collection<String> removed = lastState.graph.removeTerminatedNodes();
//
//				if(removed != null && !removed.isEmpty()){
//					return Publishers.from(
//							Arrays.asList(lastState, new RemovedGraphEvent(server.getListenAddress().getHostName(), removed)));
//				}

				return lastState;
			}
			return event;
		}

		@Override
		public String getName() {
			return "ScanIfGraphEvent";
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

	private static final class FederatedMerger implements Function<FederatedClient, Publisher<Buffer>> {

		private final HttpChannel<Buffer, Buffer> c;

		public FederatedMerger(HttpChannel<Buffer, Buffer> c) {
			this.c = c;
		}

		@Override
		public Publisher<Buffer> apply(FederatedClient o) {
			return o.client.ws(o.targetAPI)
			               .flatMap(new Function<HttpChannel<Buffer, Buffer>, Publisher<Buffer>>() {
				               @Override
				               public Publisher<Buffer> apply(HttpChannel<Buffer, Buffer> channel) {
					               return channel.input();
				               }
			               });
		}
	}

	private static final class FederatedClient {

		private final HttpClient<Buffer, Buffer> client;
		private final String                     targetAPI;

		public FederatedClient(String targetAPI) {
			this.targetAPI = targetAPI;
			this.client = ReactiveNet.httpClient();
		}
	}
}
