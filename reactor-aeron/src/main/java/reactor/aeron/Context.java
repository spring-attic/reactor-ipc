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
package reactor.aeron;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import reactor.aeron.publisher.AeronProcessor;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.utils.AeronInfra;
import reactor.aeron.utils.AeronUtils;
import reactor.aeron.utils.BasicAeronInfra;
import reactor.aeron.utils.BasicExceptionSerializer;
import reactor.aeron.utils.Serializer;
import reactor.util.Logger;
import reactor.util.Loggers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

/**
 *
 * Settings for {@link AeronSubscriber}, {@link AeronProcessor}, {@link reactor.aeron.publisher.AeronFlux#listenOn(Context)}
 *
 * @since 2.5
 */
public class Context {

	public static final int DEFAULT_SIGNAL_STREAM_ID = 1;

	/**
	 * Used as a prefix for names of created threads
	 */
	private String name;

	/**
	 * If the processor should cancel an upstream subscription when
	 * the last subscriber terminates
	 */
	private boolean autoCancel;

	private String senderChannel;

	private String receiverChannel;

	/**
	 * Aeron StreamId used by the signals sender to publish Next and
	 * Complete signals
	 */
	private int streamId = DEFAULT_SIGNAL_STREAM_ID;

	/**
	 * Aeron StreamId used by the signals sender to listen to service requests from the receiver
	 */
	private int serviceRequestStreamId = DEFAULT_SIGNAL_STREAM_ID + 1;

	/**
	 * Instance of Aeron to be used by the processor
	 */
	private Aeron aeron;

	/**
	 * Number of fragments that could be read by the signals receiver during
	 * a single call to {@link uk.co.real_logic.aeron.Subscription#poll(FragmentHandler, int)}
	 * method
	 */
	private int signalPollerFragmentLimit = 64;

	/**
	 * Number of fragments that could be read by the service message receiver during
	 * a single call to {@link uk.co.real_logic.aeron.Subscription#poll(FragmentHandler, int)}
	 * method
	 */
	private int serviceMessagePollerFragmentLimit = 1;

	/**
	 * A timeout during which a message is retied to be published into Aeron.
	 * If the timeout elapses and the message cannot be published the corresponding
	 * {@link reactor.aeron.utils.SignalPublicationFailedException},
	 * {@link reactor.aeron.utils.ServiceMessagePublicationFailedException}
	 * depending on the message type is provided into
	 * {@link #errorConsumer}
	 */
	private long publicationRetryMillis = 1000;

	/**
	 * Size of an internal ring buffer used for processing of messages
	 * to be published into Aeron
	 */
	private int ringBufferSize = 8192;

	/**
	 * Consumer of errors happened
	 */
	private Consumer<Throwable> errorConsumer = new LoggingErrorConsumer();

	/**
	 * Heartbeat interval in milliseconds
	 */
	private long heartbeatIntervalMillis = TimeUnit.SECONDS.toMillis(4);

	private final Serializer<Throwable> exceptionSerializer = new BasicExceptionSerializer();

	/**
	 * Number of signals requested from upstream by multicast sender functionality in a single call
	 * to the upstream subscription
	 */
	private final long multicastUpstreamRequest = 128;

	/**
	 * Max number of heartbeat publication failures after which the publisher is shutdown
	 */
	private int maxHeartbeatPublicationFailures = 2;

	private AeronInfra aeronInfra;

	static class LoggingErrorConsumer implements Consumer<Throwable> {

		private static final Logger logger = Loggers.getLogger(LoggingErrorConsumer.class);

		@Override
		public void accept(Throwable t) {
			logger.error("Unexpected exception", t);
		}

	}

	/**
	 * Creates a new instance of {@link Context}
	 *
	 * @return a new {@link Context}
     */
	public static Context create() {
		return new Context();
	}

	protected Context() {
	}

	public Context name(String name) {
		this.name = name;
		return this;
	}

	public Context autoCancel(boolean autoCancel) {
		this.autoCancel = autoCancel;
		return this;
	}

	public Context senderChannel(String senderChannel) {
		if(!AeronUtils.isUnicastChannel(senderChannel)){
			throw new IllegalArgumentException("senderChannel should be a unicast channel");
		}

		this.senderChannel = senderChannel;
		return this;
	}

	public Context receiverChannel(String receiverChannel) {
		this.receiverChannel = receiverChannel;
		return this;
	}

	public Context streamId(int streamId) {
		this.streamId = streamId;
		return this;
	}

	public Context aeron(Aeron aeron) {
		this.aeron = aeron;
		return this;
	}

	public Context signalPollerFragmentLimit(int limit) {
		if(limit <= 0){
			throw new IllegalArgumentException("limit should be > 0");
		}
		this.signalPollerFragmentLimit = limit;
		return this;
	}

	public Context serviceMessagePollerFragmentLimit(int limit) {
		if(limit <= 0){
			throw new IllegalArgumentException("limit should be > 0");
		}
		this.serviceMessagePollerFragmentLimit = limit;
		return this;
	}

	public Context serviceRequestStreamId(int serviceRequestStreamId) {
		this.serviceRequestStreamId = serviceRequestStreamId;
		return this;
	}

	public Context publicationRetryMillis(long publicationRetryMillis) {
		this.publicationRetryMillis = publicationRetryMillis;
		return this;
	}

	public Context ringBufferSize(int ringBufferSize) {
		this.ringBufferSize = ringBufferSize;
		return this;
	}

	public Context errorConsumer(Consumer<Throwable> errorConsumer) {
		this.errorConsumer = errorConsumer;
		return this;
	}

	public Context heartbeatIntervalMillis(long heartbeatIntervalMillis) {
		this.heartbeatIntervalMillis = heartbeatIntervalMillis;
		return this;
	}

	public void validate() {
		Objects.requireNonNull(senderChannel, "'senderChannel' should be provided");
		if(streamId == serviceRequestStreamId){
			throw new IllegalArgumentException("streamId should != serviceRequestStreamId");
		}
	}

	public String name() {
		return name;
	}

	public boolean autoCancel() {
		return autoCancel;
	}

	public String senderChannel() {
		return senderChannel;
	}

	public String receiverChannel() {
		return receiverChannel;
	}

	public int streamId() {
		return streamId;
	}

	public int serviceRequestStreamId() {
		return serviceRequestStreamId;
	}

	public Aeron aeron() {
		return aeron;
	}

	public int signalPollerFragmentLimit() {
		return signalPollerFragmentLimit;
	}

	public int serviceMessagePollerFragmentLimit() {
		return serviceMessagePollerFragmentLimit;
	}

	public long publicationRetryMillis() {
		return publicationRetryMillis;
	}

	public int ringBufferSize() {
		return ringBufferSize;
	}

	public Consumer<Throwable> errorConsumer() {
		return errorConsumer;
	}

	public long heartbeatIntervalMillis() {
		return heartbeatIntervalMillis;
	}

	public Serializer<Throwable> exceptionSerializer() {
		return exceptionSerializer;
	}

	public long multicastUpstreamRequest() {
		return multicastUpstreamRequest;
	}

	public int maxHeartbeatPublicationFailures() {
		return maxHeartbeatPublicationFailures;
	}

	public AeronInfra aeronInfra() {
		if (aeronInfra == null) {
			aeronInfra = new BasicAeronInfra(aeron, publicationRetryMillis);
			aeronInfra.initialise();
		}
		return aeronInfra;
	}

}
