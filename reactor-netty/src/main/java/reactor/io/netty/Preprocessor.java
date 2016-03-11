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

package reactor.io.netty;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.io.ipc.RemoteFlux;
import reactor.io.ipc.RemoteFluxHandler;

/**
 * A {@link RemoteFlux} callback that is attached on {@link ReactivePeer} or {@link ReactiveClient} initialization
 * and receives
 * all connected {@link RemoteFlux}. The {@link #apply} implementation must return a Publisher to complete or error
 * in order to close the {@link RemoteFlux}.
 *
 * @param <IN>  the type of the received data
 * @param <OUT> the type of replied data
 * @author Stephane Maldini
 * @since 2.5
 */
public interface Preprocessor<IN, OUT, CONN extends RemoteFlux<IN, OUT>,
		NEWIN, NEWOUT, NEWCONN extends RemoteFlux<NEWIN, NEWOUT>>
		extends Function<CONN, NEWCONN> {

	/**
	 * Preprocessed handler invoker
	 * @param <IN>
	 * @param <OUT>
	 * @param <CONN>
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 */
	final class PreprocessedHandler<IN, OUT, CONN extends RemoteFlux<IN,OUT>, NEWIN, NEWOUT, NEWCONN extends RemoteFlux<NEWIN, NEWOUT>>
			implements RemoteFluxHandler<IN, OUT, CONN> {

		private final Function<? super CONN, ? extends NEWCONN> preprocessor;
		private final RemoteFluxHandler<NEWIN, NEWOUT, NEWCONN>
				handler;

		/**
		 * Prepare a preprocessed handled
		 *
		 * @param handler
		 * @param preprocessor
		 * @param <IN>
		 * @param <OUT>
		 * @param <CONN>
		 * @param <NEWIN>
		 * @param <NEWOUT>
		 * @param <NEWCONN>
		 * @return
		 */
		public static <IN, OUT, CONN extends RemoteFlux<IN,OUT>, NEWIN, NEWOUT, NEWCONN extends RemoteFlux<NEWIN, NEWOUT>> PreprocessedHandler<IN, OUT, CONN, NEWIN, NEWOUT,NEWCONN> create(
				RemoteFluxHandler<NEWIN, NEWOUT, NEWCONN> handler,
				Function<? super CONN, ? extends NEWCONN> preprocessor
		){
			return new PreprocessedHandler<>(preprocessor, handler);
		}

		private PreprocessedHandler(Function<? super CONN, ? extends NEWCONN> preprocessor,
				RemoteFluxHandler<NEWIN, NEWOUT, NEWCONN> handler) {
			this.preprocessor = preprocessor;
			this.handler = handler;
		}

		@Override
		public Publisher<Void> apply(CONN conn) {
			return handler.apply(preprocessor.apply(conn));
		}
	}
}
