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

package reactor.io.netty.tcp;

import java.net.InetSocketAddress;
import java.util.function.Function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.state.Introspectable;
import reactor.core.timer.Timer;
import reactor.core.tuple.Tuple2;
import reactor.io.netty.Preprocessor;
import reactor.io.ipc.RemoteFlux;
import reactor.io.ipc.RemoteFluxHandler;
import reactor.io.netty.ReactiveClient;
import reactor.io.netty.ReactivePeer;
import reactor.io.netty.Reconnect;
import reactor.io.netty.config.ClientSocketOptions;
import reactor.io.netty.config.SslOptions;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpClient<IN, OUT>
		extends ReactiveClient<IN, OUT, RemoteFlux<IN, OUT>>
		implements Introspectable {

	private final InetSocketAddress   connectAddress;
	private final ClientSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpClient(Timer timer,
	                    InetSocketAddress connectAddress,
	                    ClientSocketOptions options,
	                    SslOptions sslOptions) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.connectAddress = (null != connectAddress ? connectAddress : new InetSocketAddress("127.0.0.1", 3000));
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Get the {@link InetSocketAddress} to which this client must connect.
	 *
	 * @return the connect address
	 */
	public InetSocketAddress getConnectAddress() {
		return connectAddress;
	}

	/**
	 * Get the {@link ClientSocketOptions} currently in effect.
	 *
	 * @return the client options
	 */
	protected ClientSocketOptions getOptions() {
		return this.options;
	}

	/**
	 * Get the {@link SslOptions} current in effect.
	 *
	 * @return the SSL options
	 */
	protected SslOptions getSslOptions() {
		return sslOptions;
	}

	@Override
	public String getName() {
		return "TcpClient:"+getConnectAddress().toString();
	}

	@Override
	public int getMode() {
		return 0;
	}

	@Override
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, RemoteFlux<NEWIN, NEWOUT>> doPreprocessor(
			Function<RemoteFlux<IN, OUT>, ? extends RemoteFlux<NEWIN, NEWOUT>> preprocessor) {
		return new PreprocessedTcpClient<>(preprocessor);
	}

	private final class PreprocessedTcpClient<NEWIN, NEWOUT, NEWCONN extends RemoteFlux<NEWIN, NEWOUT>>
			extends TcpClient<NEWIN, NEWOUT> {

		private final Function<RemoteFlux<IN, OUT>, ? extends NEWCONN>
				preprocessor;

		public PreprocessedTcpClient(Function<RemoteFlux<IN, OUT>, ? extends NEWCONN> preprocessor) {
			super(TcpClient.this.getDefaultTimer(), TcpClient.this.getConnectAddress(), TcpClient.this.getOptions(), TcpClient.this.getSslOptions());
			this.preprocessor = preprocessor;
		}

		@Override
		protected Mono<Void> doStart(
				RemoteFluxHandler<NEWIN, NEWOUT, RemoteFlux<NEWIN, NEWOUT>> handler) {
			RemoteFluxHandler<IN, OUT, RemoteFlux<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p);
		}

		@Override
		protected Flux<Tuple2<InetSocketAddress, Integer>> doStart(
				RemoteFluxHandler<NEWIN, NEWOUT, RemoteFlux<NEWIN, NEWOUT>> handler,
				Reconnect reconnect) {
			RemoteFluxHandler<IN, OUT, RemoteFlux<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p, reconnect);
		}

		@Override
		protected Mono<Void> doShutdown() {
			return TcpClient.this.shutdown();
		}

		@Override
		protected boolean shouldFailOnStarted() {
			return false;
		}
	}
}
