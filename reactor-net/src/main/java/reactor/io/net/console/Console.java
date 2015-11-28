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

package reactor.io.net.console;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class Console extends ReactivePeer<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>
		implements ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> {

	private static final String CONSOLE_STATIC_PATH     = "/META-INF/reactor/console/";
	private static final String CONSOLE_URL             = "/console";
	private static final String API_STREAM_URL          = "/api/stream";
	private static final String CONSOLE_JS_URL          = "/console/console.js";
	private static final String CONSOLE_CSS_URL         = "/console/console.css";
	private static final String VIS_JS_URL              = "/console/vis.min.js";
	private static final String VIS_CSS_URL             = "/console/vis.min.css";
	private static final String CSS_DEPENDENCY_VIS      = "vis.min.css";
	private static final String JS_DEPENDENCY_VIS       = "vis.min.js";
	private static final String JS_DEPENDENCY_CONSOLE   = "console.js";
	private static final String CSS_DEPENDENCY_CONSOLE  = "console.css";
	private static final String HTML_DEPENDENCY_CONSOLE = "console.html";

	private final HttpServer<Buffer, Buffer> server;
	private final JsonCodec<Map, Map>        jsonCodec;
	private final Map<String, Object> metrics = new HashMap<>();



	/**
	 *
	 * @param server
	 * @return
	 */
	public static Console create(HttpServer<Buffer, Buffer> server) {

		Console console = new Console(server.getDefaultTimer(), server);

		server.file(CONSOLE_URL,
				Console.class.getResource(CONSOLE_STATIC_PATH + HTML_DEPENDENCY_CONSOLE)
				             .getPath())
		      .file(CONSOLE_JS_URL,
				      Console.class.getResource(CONSOLE_STATIC_PATH + JS_DEPENDENCY_CONSOLE)
				                   .getPath())
		      .file(CONSOLE_CSS_URL,
				      Console.class.getResource(CONSOLE_STATIC_PATH + CSS_DEPENDENCY_CONSOLE)
				                   .getPath())
		      .file(VIS_JS_URL,
				      Console.class.getResource(CONSOLE_STATIC_PATH + JS_DEPENDENCY_VIS)
				                   .getPath())
		      .file(VIS_CSS_URL,
				      Console.class.getResource(CONSOLE_STATIC_PATH + CSS_DEPENDENCY_VIS)
				                   .getPath())
		      .get(API_STREAM_URL, console);

		return console;
	}

	private Console(Timer defaultTimer, HttpServer<Buffer, Buffer> server) {
		super(defaultTimer);
		this.server = server;
		this.jsonCodec = new JsonCodec<>(Map.class);

		//Mock
		List<Map<String, String>> nodes = new ArrayList<>();
		List<Map<String, String>> edges = new ArrayList<>();
		metrics.put("streams", nodes);
		metrics.put("stream_edges", edges);

		String[] op = new String[]{"map", "filter", "scan"};

		Map<String, String> object;
		int n = 10;
		for(int i = 0 ; i < n; i++) {
			object = new HashMap<>();
			object.put("id", ""+i);
			object.put("label", ""+ ( i == 0 ? "Iterable" : (i == n - 1 ? "Consumer" : op[i % 3])));
			nodes.add(object);

			if( i < n - 1) {
				object = new HashMap<>();
				object.put("id", "" + i);
				object.put("from", "" + i);
				object.put("to", "" + (i + 1));
				edges.add(object);

				if ( i % 3 == 0){
					object = new HashMap<>();
					object.put("id", ""+(i + 100));
					object.put("label", "Consumer");
					nodes.add(object);

					object = new HashMap<>();
					object.put("id", "" + (i + 100));
					object.put("from", "" + i);
					object.put("to", "" + (i + 100));
					edges.add(object);
				}
			}


		}


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
		return channel.writeWith(jsonCodec.encode(Publishers.just(metrics)));
	}

	public HttpServer<Buffer, Buffer> getServer() {
		return server;
	}
}
