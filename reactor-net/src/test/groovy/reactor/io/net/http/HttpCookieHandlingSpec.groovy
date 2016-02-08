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
package reactor.io.net.http

import reactor.core.publisher.Mono
import reactor.io.net.http.model.Cookie
import reactor.io.net.preprocessor.CodecPreprocessor
import reactor.rx.net.NetStreams
import reactor.rx.net.http.HttpChannelStream
import spock.lang.Specification

import java.util.concurrent.TimeUnit

public class HttpCookieHandlingSpec extends Specification{

  def "client without cookie gets a new one from server"(){

	def getResponseCookie = {
	  new Cookie(){

		@Override
		String name() {
		  'cookie1'
		}

		@Override
		String domain() {
		  '/'
		}

		@Override
		String path() {
		  '/'
		}

		@Override
		String value() {
		  'test_value'
		}
	  }
	}

	given: "a http server setting cookie 1.0"
	def server = NetStreams.httpServer {
	  it.httpProcessor(CodecPreprocessor.string()).listen(0)
	}

	when: "server is prepared"
	server.get("/test"){
	  HttpChannelStream<String,String> req ->
		req.addResponseCookie("cookie1", getResponseCookie())
			.writeWith(req.log("server received"))
	}

	then: "the server was started"
	server
	!server.start().get(5, TimeUnit.SECONDS)

	when: "a request with URI is sent onto the server"
	def client = NetStreams.httpClient {
	  it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
	}
	def cookieResponse = client.get('/test').then {
	  replies -> Mono.just(replies.cookies())
	}
	.doOnSuccess{
	  println it
	}
	.doOnError {
	  println "Failed requesting server: $it"
	}

	then: "data with cookies was received"
	def value = ""
	try {
	  def receivedCookies = cookieResponse.get(5, TimeUnit.SECONDS)
	  value = receivedCookies.get("cookie1")[0].value()
	} catch (RuntimeException ex) {
	  println ex.getMessage();
	}
	value == "test_value"

	cleanup: "the client/server where stopped"
	client?.shutdown()
	server?.shutdown()
  }

}
