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

package reactor.io.netty.config;

import java.net.InetSocketAddress;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import reactor.core.scheduler.TimedScheduler;
import reactor.io.netty.common.Peer;

/**
 * @author Stephane Maldini
 */
public class ClientOptions extends NettyOptions<ClientOptions> {

	public static ClientOptions create(){
		return new ClientOptions();
	}

	public static ClientOptions to(String host){
		return to(host, Peer.DEFAULT_PORT);
	}

	public static ClientOptions to(String host, int port){
		return new ClientOptions().connect(host, port);
	}


	private Supplier<? extends InetSocketAddress> connectAddress;

	ClientOptions(){

	}


	/**
	 * The host and port to which this client should connect.
	 *
	 * @param host The host to connect to.
	 * @param port The port to connect to.
	 * @return {@literal this}
	 */
	public ClientOptions connect(@Nonnull String host, int port) {
		return connect(new InetSocketAddress(host, port));
	}

	/**
	 * The address to which this client should connect.
	 *
	 * @param connectAddress The address to connect to.
	 * @return {@literal this}
	 */
	public ClientOptions connect(@Nonnull InetSocketAddress connectAddress) {
		return connect(() -> connectAddress);
	}

	/**
	 * The address to which this client should connect.
	 *
	 * @param connectAddress The address to connect to.
	 * @return {@literal this}
	 */
	public ClientOptions connect(@Nonnull Supplier<? extends InetSocketAddress> connectAddress) {
		if(this.connectAddress != null) {
			throw new IllegalStateException("Connect address is already set.");
		}
		this.connectAddress = connectAddress;
		return this;
	}

	/**
	 * Return the eventual remote host
	 * @return the eventual remote host
	 */
	public InetSocketAddress remoteAddress() {
		return connectAddress.get();
	}

	/**
	 *
	 * @return immutable {@link ClientOptions}
	 */
	public ClientOptions toImmutable() {
		return new ImmutableClientOptions(this);
	}

	final static class ImmutableClientOptions extends ClientOptions {

		final ClientOptions options;

		ImmutableClientOptions(ClientOptions options) {
			this.options = options;
			if(options.ssl() != null){
					super.ssl(options.ssl().toImmutable());
			}
		}

		@Override
		public InetSocketAddress remoteAddress() {
			return options.remoteAddress();
		}

		@Override
		public EventLoopGroup eventLoopGroup() {
			return options.eventLoopGroup();
		}

		@Override
		public boolean isManaged() {
			return options.isManaged();
		}

		@Override
		public boolean isRaw() {
			return options.isRaw();
		}

		@Override
		public boolean keepAlive() {
			return options.keepAlive();
		}

		@Override
		public int linger() {
			return options.linger();
		}

		@Override
		public Consumer<ChannelPipeline> pipelineConfigurer() {
			return options.pipelineConfigurer();
		}

		@Override
		public long prefetch() {
			return options.prefetch();
		}

		@Override
		public int rcvbuf() {
			return options.rcvbuf();
		}

		@Override
		public int sndbuf() {
			return options.sndbuf();
		}

		@Override
		public boolean tcpNoDelay() {
			return options.tcpNoDelay();
		}

		@Override
		public int timeout() {
			return options.timeout();
		}

		@Override
		public TimedScheduler timer() {
			return options.timer();
		}

		@Override
		public ClientOptions connect(@Nonnull String host, int port) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions connect(@Nonnull InetSocketAddress connectAddress) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions eventLoopGroup(EventLoopGroup eventLoopGroup) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions keepAlive(boolean keepAlive) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions linger(int linger) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions managed(boolean managed) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions pipelineConfigurer(Consumer<ChannelPipeline> pipelineConfigurer) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions isRaw(boolean israw) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions prefetch(long prefetch) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions rcvbuf(int rcvbuf) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions ssl(SslOptions sslOptions) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions sndbuf(int sndbuf) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions tcpNoDelay(boolean tcpNoDelay) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions timeout(int timeout) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public ClientOptions timer(TimedScheduler timer) {
			throw new UnsupportedOperationException("Immutable Options");
		}
	}

}
