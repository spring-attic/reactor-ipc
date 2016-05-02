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

package reactor.io.netty.http;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

/**
 * @author tjreactive
 * @author smaldini
 */
public class WebsocketTests {

	private HttpServer httpServer;

	static final String auth =
			"bearer eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI1NzE4NjFhNS04YmZkLTQ1ZGEtOGExNy05NjVkZTUyOTA1ZmEiLCJzdWIiOiI0ZjI3ZDU2ZS0wYzcwLTRkZTctYTU1YS04MmYzYThmYzY2YzUiLCJzY29wZSI6WyJvcGVuaWQiLCJ1YWEudXNlciIsImNsb3VkX2NvbnRyb2xsZXIucmVhZCIsInBhc3N3b3JkLndyaXRlIiwiY2xvdWRfY29udHJvbGxlci53cml0ZSJdLCJjbGllbnRfaWQiOiJjZiIsImNpZCI6ImNmIiwiYXpwIjoiY2YiLCJncmFudF90eXBlIjoicGFzc3dvcmQiLCJ1c2VyX2lkIjoiNGYyN2Q1NmUtMGM3MC00ZGU3LWE1NWEtODJmM2E4ZmM2NmM1Iiwib3JpZ2luIjoidWFhIiwidXNlcl9uYW1lIjoiYmhhbGVAcGl2b3RhbC5pbyIsImVtYWlsIjoiYmhhbGVAcGl2b3RhbC5pbyIsImF1dGhfdGltZSI6MTQ2MTcwNTUxNywicmV2X3NpZyI6IjdjZjhkNmVhIiwiaWF0IjoxNDYxNzA1NTE3LCJleHAiOjE0NjE3MDYxMTcsImlzcyI6Imh0dHBzOi8vdWFhLnJ1bi5waXZvdGFsLmlvL29hdXRoL3Rva2VuIiwiemlkIjoidWFhIiwiYXVkIjpbImNmIiwib3BlbmlkIiwidWFhIiwiY2xvdWRfY29udHJvbGxlciIsInBhc3N3b3JkIl19.sG8fTMwni9Rg6MsbWfBr_VGjo22XP3fjrsvAqjz3bg7EZu11VoGrpNi2RKgXMwSG3GyTw8AKfz8BUq4uVfY8t2Q3Ch-CsSPAYazaRUl4FokuOl5foCve6afvP67GX6TSzcKTdKf1Wcr5dgr5OQzcH4odPEdo0mfn6ttfqdKo1vKLEYuHrLUAjb6ncwUjt8BA7l2BbWY9qZA_Nq8wE8fov9wjVri0I2-KjiPXMBwzX771LK49tqRLmfuLp-aBzxW8-9p30qRZVI30fTVHV3zIMlXzpUpPxxp2T1_0qqf3MwRW-0fB5st1qZcFrYTdDXpBWyhqsXqPBU0c3KxgjqPHMQ";

	@Before
	public void setup() throws InterruptedException {
		setupServer();
	}

	private void setupServer() throws InterruptedException {
		httpServer = HttpServer.create(0);
		httpServer.start(channel -> channel.upgradeToTextWebsocket()
		                                   .concatWith(channel.sendString(Mono.just("test"))))
		          .get();
	}

	@Test
	public void simpleTest() {
			String res = HttpClient.create("localhost", httpServer.getListenAddress().getPort())
			                       .get("/test",
					                       c -> c.addHeader("Authorization", auth)
					                             .upgradeToTextWebsocket())
			                       .flatMap(HttpInbound::receiveString)
			                       .log()
			                       .toList()
			                       .get()
		                       .get(0);

		if (!res.equals("test")) {
			throw new IllegalStateException("test");
		}
	}

	@After
	public void teardown() throws Exception {
		httpServer.shutdown();
	}


}
