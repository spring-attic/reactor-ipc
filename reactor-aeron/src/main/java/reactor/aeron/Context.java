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

import org.slf4j.Logger;
import reactor.aeron.processor.AeronProcessor;
import reactor.aeron.publisher.AeronPublisher;
import reactor.aeron.subscriber.AeronSubscriber;
import reactor.aeron.support.AeronInfra;
import reactor.aeron.support.AeronUtils;
import reactor.aeron.support.BasicAeronInfra;
import reactor.aeron.support.BasicExceptionSerializer;
import reactor.aeron.support.LoggingErrorConsumer;
import reactor.aeron.support.Serializer;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A class containing parameter values required to create instances of
 * {@link AeronProcessor}, {@link AeronSubscriber} or {@link AeronPublisher}
 */
public class Context {

	public static final int DEFAULT_SENDER_PORT = 12000;

	public static final int DEFAULT_RECEIVER_PORT = 12001;

	public static final int DEFAULT_SIGNAL_STREAM_ID = 1;

	/**
	 * Processor name used as a base name for threads created by
	 * the processor's executor
	 */
	private String name;

	/**
	 * If the processor should cancel an upstream subscription when
	 * the last subscriber terminates
	 */
	private boolean autoCancel;

	private String senderChannel = createSenderChannelForPort(DEFAULT_SENDER_PORT);

	private String receiverChannel = createReceiverChannelForPort(DEFAULT_RECEIVER_PORT);

	/**
	 * Aeron StreamId used by the signals sender to publish Next and
	 * Complete signals
	 */
	private int streamId = DEFAULT_SIGNAL_STREAM_ID;

	/**
	 * Aeron StreamId used by the signals sender to publish Error signals
	 */
	private int errorStreamId = 2;

	/**
	 * Aeron StreamId used by the signals sender to listen to service requests from the receiver
	 */
	private int serviceRequestStreamId = 3;

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
	 * A time interval in millis after which a message is considered published via Aeron into network
	 */
	private long publicationLingerMillis =
			TimeUnit.NANOSECONDS.toMillis(Configuration.PUBLICATION_LINGER_DEFAULT_NS);

	/**
	 * A timeout during which a message is retied to be published into Aeron.
	 * If the timeout elapses and the message cannot be published because of
	 * either {@link uk.co.real_logic.aeron.Publication#BACK_PRESSURED} or
	 * {@link uk.co.real_logic.aeron.Publication#NOT_CONNECTED} it is discarded.
	 * In the next version of the processor the behaviour is likely to change.
	 */
	private long publicationRetryMillis = 1000;

	/**
	 * Size of internal ring buffer used for processing of messages
	 * to be published into Aeron
	 */
	private int ringBufferSize = RingBufferProcessor.MEDIUM_BUFFER_SIZE;

	private Consumer<Throwable> errorConsumer = new LoggingErrorConsumer();

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

	private static String createSenderChannelForPort(int port) {
		return "udp://localhost:" + port;
	}

	private static String createReceiverChannelForPort(int port) {
		return "udp://" + getLocalHostIPAddress() + ":" + port;
	}

	public static String getLocalHostIPAddress() {
		try {
			return Inet4Address.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public Context name(String name) {
		this.name = name;
		return this;
	}

	public Context autoCancel(boolean autoCancel) {
		this.autoCancel = autoCancel;
		return this;
	}

	public Context senderPort(int senderPort) {
		this.senderChannel = createSenderChannelForPort(senderPort);
		return this;
	}

	public Context senderChannel(String senderChannel) {
		Assert.isTrue(AeronUtils.isUnicastChannel(senderChannel), "senderChannel should be a unicast channel");

		this.senderChannel = senderChannel;
		return this;
	}

	public Context receiverPort(int receiverPort) {
		this.receiverChannel = createReceiverChannelForPort(receiverPort);
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

	public Context signalPollerFragmentLimit(int signalPollerFragmentLimit) {
		Assert.isTrue(signalPollerFragmentLimit > 0, "signalPollerFragmentLimit should be > 0");
		this.signalPollerFragmentLimit = signalPollerFragmentLimit;
		return this;
	}

	public Context errorStreamId(int errorStreamId) {
		this.errorStreamId = errorStreamId;
		return this;
	}

	public Context serviceRequestStreamId(int serviceRequestStreamId) {
		this.serviceRequestStreamId = serviceRequestStreamId;
		return this;
	}

	public Context publicationLingerMillis(int publicationLingerMillis) {
		this.publicationLingerMillis = publicationLingerMillis;
		return this;
	}

	public Context publicationRetryMillis(long publicationRetryMillis) {
		this.publicationRetryMillis = publicationRetryMillis;
		return this;
	}

	//TODO: Review its usage
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
		Assert.isTrue(name != null, "name should be provided");

		assertStreamIdsAreDifferent();
	}

	private void assertStreamIdsAreDifferent() {
		Set<Integer> streamIdsSet = new HashSet<>();
		streamIdsSet.add(streamId);
		streamIdsSet.add(errorStreamId);
		streamIdsSet.add(serviceRequestStreamId);

		Assert.isTrue(streamIdsSet.size() == 3,
				String.format("streamId: %d, errorStreamId: %d, serviceRequestStreamId: %d"
								+ " should all be different",
						streamId, errorStreamId, serviceRequestStreamId));
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

	public int errorStreamId() {
		return errorStreamId;
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

	public long publicationLingerMillis() {
		return publicationLingerMillis;
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

	//TODO: Move into another class
	public AeronInfra createAeronInfra(Logger logger) {
		AeronInfra aeronInfra = new BasicAeronInfra(logger, aeron, 	publicationRetryMillis, publicationLingerMillis);
		aeronInfra.initialise();
		return aeronInfra;
	}

}
