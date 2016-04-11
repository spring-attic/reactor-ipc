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

package reactor.io.netty.nexus;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import reactor.core.flow.Loopback;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SchedulerGroup;
import reactor.core.publisher.TopicProcessor;
import reactor.core.state.Introspectable;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.subscriber.Subscribers;
import reactor.core.scheduler.Timer;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;
import reactor.core.util.WaitStrategy;
import reactor.io.buffer.Buffer;
import reactor.io.ipc.ChannelFlux;
import reactor.io.ipc.ChannelFluxHandler;
import reactor.io.netty.ReactiveNet;
import reactor.io.netty.ReactivePeer;
import reactor.io.netty.http.HttpChannel;
import reactor.io.netty.http.HttpClient;
import reactor.io.netty.http.HttpServer;
import reactor.io.netty.http.NettyHttpServer;
import reactor.io.netty.tcp.TcpServer;

import static reactor.core.util.ReactiveStateUtils.property;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Nexus extends ReactivePeer<Buffer, Buffer, ChannelFlux<Buffer, Buffer>>
		implements ChannelFluxHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>>, Loopback {

	/**
	 * Bind a new TCP server to "loopback" on the given port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on loopback
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus create(int port) {
		return create(ReactiveNet.DEFAULT_BIND_ADDRESS, port);
	}

	/**
	 * Bind a new TCP server to the given bind address on port {@literal 12012}. By default the default server
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of
	 * {@link Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from -
	 * onComplete when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted
	 * {@link ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data
	 * on the active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the default port 12012
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus create(String bindAddress) {
		return create(bindAddress, ReactiveNet.DEFAULT_PORT);
	}

	/**
	 * Bind a new TCP server to the given bind address and port. By default the default server implementation is scanned
	 * from the classpath on Class init. Support for Netty is provided as long as the relevant
	 * library dependencies are on the classpath. <p> A {@link TcpServer} is a specific kind of {@link
	 * Publisher} that will emit: - onNext {@link ChannelFlux} to consume data from - onComplete
	 * when server is shutdown - onError when any error (more specifically IO error) occurs From the emitted {@link
	 * ChannelFlux}, one can decide to add in-channel consumers to read any incoming data. <p> To reply data on the
	 * active connection, {@link ChannelFlux#writeWith} can subscribe to any passed {@link
	 * Publisher}. <p> Note that {@link reactor.core.state.Backpressurable#getCapacity} will be used to
	 * switch on/off a channel in auto-read / flush on write mode. If the capacity is Long.MAX_Value, write on flush and
	 * auto read will apply. Otherwise, data will be flushed every capacity batch size and read will pause when capacity
	 * number of elements have been dispatched. <p> Emitted channels will run on the same thread they have beem
	 * receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @param port the port to listen on the passed bind address
	 * @param bindAddress bind address (e.g. "127.0.0.1") to create the server on the passed port
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus create(final String bindAddress, final int port) {
		return ReactiveNet.nexus(serverSpec -> {
			serverSpec.timer(Timer.globalOrNull());
			return serverSpec.listen(bindAddress, port);
		});
	}

	/**
	 * Bind a new Console HTTP server to "loopback" on port {@literal 12012}. By default the default server
	 * implementation is scanned from the classpath on Class init. Support for Netty is provided
	 * as long as the relevant library dependencies are on the classpath. <p> To reply data on the active connection,
	 * {@link ChannelFlux#writeWith} can subscribe to any passed {@link Publisher}. <p> Note
	 * that {@link reactor.core.state.Backpressurable#getCapacity} will be used to switch on/off a channel in auto-read /
	 * flush on write mode. If the capacity is Long.MAX_Value, write on flush and auto read will apply. Otherwise, data
	 * will be flushed every capacity batch size and read will pause when capacity number of elements have been
	 * dispatched. <p> Emitted channels will run on the same thread they have beem receiving IO events.
	 *
	 * <p> By default the type of emitted data or received data is {@link Buffer}
	 * @return a new Stream of ChannelFlux, typically a peer of connections.
	 */
	public static Nexus create() {
		return Nexus.create(ReactiveNet.DEFAULT_BIND_ADDRESS);
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

	public static void main(String... args) throws Exception {
		log.info("Deploying Nexus... ");

		Nexus nexus = create();

		final CountDownLatch stopped = new CountDownLatch(1);

		nexus.startAndAwait();

		log.info("CTRL-C to return...");
		stopped.await();
	}
	private final HttpServer<Buffer, Buffer>        server;
	private final GraphEvent                        lastState;
	private final SystemEvent                       lastSystemState;
	private final FluxProcessor<Event, Event>       eventStream;
	private final SchedulerGroup                    group;
	private final Function<Event, Event>            lastStateMerge;
	private final Timer                             timer;
	private final SignalEmitter<Publisher<Event>> cannons;

	@SuppressWarnings("unused")
	private volatile FederatedClient[] federatedClients;
	private long                 systemStatsPeriod;
	private boolean              systemStats;
	private boolean              logExtensionEnabled;
	private NexusLoggerExtension logExtension;
	private long websocketCapacity = 1L;

	private Nexus(Timer defaultTimer, HttpServer<Buffer, Buffer> server) {
		super(defaultTimer);
		this.server = server;
		this.eventStream = EmitterProcessor.create(false);
		this.lastStateMerge = new LastGraphStateMap();
		this.timer = Timer.create("create-poller");
		this.group = SchedulerGroup.async("create", 1024, 1, null, null, false, new Supplier<WaitStrategy>() {
			@Override
			public WaitStrategy get() {
				return WaitStrategy.blocking();
			}
		});

		FluxProcessor<Publisher<Event>, Publisher<Event>> cannons = EmitterProcessor.create();

		Flux.merge(cannons)
		    .subscribe(eventStream);

		this.cannons = cannons.connectEmitter();

		lastState = new GraphEvent(server.getListenAddress()
		                                 .toString(), ReactiveStateUtils.createGraph());

		lastSystemState = new SystemEvent(server.getListenAddress()
		                                        .toString());
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

	@Override
	public Object connectedInput() {
		return eventStream;
	}

	@Override
	public Object connectedOutput() {
		return server;
	}

	/**
	 * @return
	 */
	public final Nexus disableLogTail() {
		this.logExtensionEnabled = false;
		return this;
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

	/**
	 * @return
	 */
	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}

	/**
	 * @return
	 */
	public final SignalEmitter<Object> metricCannon() {
		FluxProcessor<Object, Object> p = FluxProcessor.blocking();
		this.cannons.submit(p.map(new MetricMapper()));
		return p.connectEmitter();
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

		FluxProcessor<Object, Object> p = FluxProcessor.blocking();
		final SignalEmitter<Object> session = p.connectEmitter();
		log.info("State Monitoring Starting on " + ReactiveStateUtils.getName(o));
		timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				if (!session.isCancelled()) {
					session.emit(ReactiveStateUtils.scan(o));
				}
				else {
					log.info("State Monitoring stopping on " + ReactiveStateUtils.getName(o));
					throw Exceptions.failWithCancel();
				}
			}
		}, _period);

		this.cannons.submit(p.map(new GraphMapper()));

		return o;
	}

	/**
	 * @see this#start(ChannelFluxHandler)
	 */
	public final Mono<Void> start() throws InterruptedException {
		return start(null);
	}

	/**
	 * @see this#start(ChannelFluxHandler)
	 */
	public final void startAndAwait() throws InterruptedException {
		start().get();
		InetSocketAddress addr = server.getListenAddress();
		log.info("Nexus Warped. Transmitting signal to troops under http://" + addr.getHostName() + ":" + addr.getPort() +
				API_STREAM_URL);
	}

	/**
	 * @return
	 */
	public final SignalEmitter<Object> streamCannon() {
		FluxProcessor<Object, Object> p = FluxProcessor.blocking();
		this.cannons.submit(p.map(new GraphMapper()));
		return p.connectEmitter();
	}

	/**
	 * @return
	 */
	public final Nexus useCapacity(long capacity) {
		this.websocketCapacity = capacity;
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

	@Override
	protected Mono<Void> doStart(ChannelFluxHandler<Buffer, Buffer, ChannelFlux<Buffer, Buffer>> handler) {

		if (logExtensionEnabled) {
			FluxProcessor<Event, Event> p =
					TopicProcessor.share("create-log-sink", 256, WaitStrategy.blocking());
			cannons.submit(p);
			logExtension = new NexusLoggerExtension(server.getListenAddress()
			                                              .toString(), p.connectEmitter());

			//monitor(p);
			if (!Logger.enableExtension(logExtension)) {
				log.warn("Couldn't setup logger extension as one is already in place");
				logExtension = null;
			}
		}

		if (systemStats) {
			FluxProcessor<Event, Event> p = FluxProcessor.blocking();
			this.cannons.submit(p);
			final SignalEmitter<Event> session = p.connectEmitter();
			log.info("System Monitoring Starting");
			timer.schedule(new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					if (!session.isCancelled()) {
						session.submit(lastSystemState.scan());
					}
					else {
						log.info("System Monitoring Stopped");
						throw Exceptions.failWithCancel();
					}
				}
			}, systemStatsPeriod);
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

	private Flux<? extends Buffer> federateAndEncode(HttpChannel<Buffer, Buffer> c, Flux<Event> stream) {
		FederatedClient[] clients = federatedClients;
		if (clients == null || clients.length == 0) {
			return stream.map(BUFFER_STRING_FUNCTION)
			             .useCapacity(websocketCapacity);
		}
		Flux<Buffer> mergedUpstreams = Flux.merge(Flux.fromArray(clients)
		                                              .map(new FederatedMerger(c)));

		return Flux.merge(stream.map(BUFFER_STRING_FUNCTION), mergedUpstreams)
		           .useCapacity(websocketCapacity);
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

		public String getCategory() {
			return category;
		}

		public String getData() {
			return data;
		}

		public String getKind() {
			return kind;
		}

		public Level getLevel() {
			return level;
		}

		public String getMessage() {
			return message;
		}

		public String getOrigin() {
			return origin;
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

		private final Map<Thread, ThreadState> threads = new WeakHashMap<>();
		public SystemEvent(String hostname) {
			super(hostname);
		}

		public JvmStats getJvmStats() {
			return jvmStats;
		}

		public Collection<ThreadState> getThreads() {
			return threads.values();
		}

		@Override
		public String toString() {
			return "{ " + property("jvmStats", getJvmStats()) +
					", " + property("threads", getThreads()) +
					", " + property("type", getType()) +
					", " + property("timestamp", System.currentTimeMillis()) +
					", " + property("nexusHost", getNexusHost()) + " }";
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

			public int getActiveThreads() {
				return Thread.activeCount();
			}

			public int getAvailableProcessors() {
				return runtime.availableProcessors();
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

			public String getName() {
				return thread.getName();
			}

			public int getPriority() {
				return thread.getPriority();
			}

			public Thread.State getState() {
				return thread.getState();
			}

			public String getThreadGroup() {
				ThreadGroup group = thread.getThreadGroup();
				return group != null ? thread.getThreadGroup()
				                             .getName() : null;
			}

			public boolean isAlive() {
				return thread.isAlive();
			}

			public boolean isDaemon() {
				return thread.isDaemon();
			}

			public boolean isInterrupted() {
				return thread.isInterrupted();
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
		private static final Runtime  runtime  = Runtime.getRuntime();
		private static final JvmStats jvmStats = new JvmStats();
	}

	private static class StringToBuffer implements Function<Event, Buffer> {

		@Override
		public Buffer apply(Event event) {
			try {
				return reactor.io.buffer.StringBuffer.wrap(event.toString()
				                                                .getBytes("UTF-8"));
			}
			catch (UnsupportedEncodingException e) {
				throw Exceptions.propagate(e);
			}
		}
	}

	private final static class NexusLoggerExtension implements Logger.Extension {

		final SignalEmitter<Event> logSink;
		final String                 hostname;

		public NexusLoggerExtension(String hostname, SignalEmitter<Event> logSink) {
			this.logSink = logSink;
			this.hostname = hostname;
		}

		@Override
		public void log(String category, Level level, String msg, Object... arguments) {
			String computed = Logger.format(msg, arguments);
			SignalEmitter.Emission emission;

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
			this.client = HttpClient.create();
		}
	}
	static final AtomicReferenceFieldUpdater<Nexus, FederatedClient[]> FEDERATED =
			PlatformDependent.newAtomicReferenceFieldUpdater(Nexus.class, "federatedClients");
	private static final Logger log            = Logger.getLogger(Nexus.class);
	private static final String API_STREAM_URL = "/create/stream";
	private static final Function<Event, Buffer> BUFFER_STRING_FUNCTION = new StringToBuffer();

	private class LastGraphStateMap implements Function<Event, Event>, Introspectable {

		@Override
		public Event apply(Event event) {
			if (GraphEvent.class.equals(event.getClass())) {
				lastState.graph.mergeWith(((GraphEvent) event).graph);
//				Collection<String> removed = lastState.graph.removeTerminatedNodes();
//
//				if(removed != null && !removed.isEmpty()){
//					return Flux.from(
//							Arrays.asList(lastState, new RemovedGraphEvent(server.getListenAddress().getHostName(), removed)));
//				}

				return lastState;
			}
			return event;
		}

		@Override
		public int getMode() {
			return 0;
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
}
