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
import reactor.core.util.Exceptions
import reactor.io.net.impl.netty.http.NettyHttpServer
import reactor.io.net.preprocessor.CodecPreprocessor
import reactor.rx.Fluxion
import reactor.rx.net.NetStreams
import reactor.rx.net.http.HttpChannelFluxion
import rx.Observable
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 */
class HttpSpec extends Specification {

  def "http responds empty"() {
	given: "a simple HttpServer"

	//Listen on localhost using default impl (Netty) and assign a global codec to receive/reply String data
	def server = NetStreams.httpServer {
	  it.httpProcessor(CodecPreprocessor.string()).listen(0)
	}

	when: "the server is prepared"

	//prepare post request consumer on /test/* and capture the URL parameter "param"
	server.post('/test/{param}') {
	  HttpChannelFluxion<String, String> req
		->

		//log then transform then log received http request content from the request body and the resolved URL parameter "param"
		//the returned stream is bound to the request stream and will auto read/close accordingly

		req.writeWith(Fluxion.empty())

	}

	then: "the server was started"
	server
	!server.start().get(5, TimeUnit.SECONDS)

	when: "data is sent with Reactor HTTP support"

	//Prepare a client using default impl (Netty) to connect on http://localhost:port/ and assign global codec to send/receive String data
	def client = NetStreams.httpClient {
	  it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
	}

	//prepare an http post request-reply flow
	def content = client.post('/test/World') { HttpChannelFluxion<String, String> req ->
	  //prepare content-type
	  req.header('Content-Type', 'text/plain')

	  //return a producing stream to send some data along the request
	  req.writeWith(Mono.just("Hello").log('client-send'))

	}.then { replies ->
	  //successful request, listen for the first returned next reply and pass it downstream
	  replies.log('client-received').next()
	}
	.doOnError {
	  //something failed during the request or the reply processing
	  println "Failed requesting server: $it"
	}

	then: "data was not recieved"
	//the produced reply should be there soon
	!content.get(5, TimeUnit.SECONDS)
  }


	def "http responds to requests from clients"() {
	given: "a simple HttpServer"

	//Listen on localhost using default impl (Netty) and assign a global codec to receive/reply String data
	def server = NetStreams.httpServer {
	  it.httpProcessor(CodecPreprocessor.string()).listen(0)
	}

	when: "the server is prepared"

	//prepare post request consumer on /test/* and capture the URL parameter "param"
	server.post('/test/{param}') {
	  HttpChannelFluxion<String, String> req
		->

		//log then transform then log received http request content from the request body and the resolved URL parameter "param"
		//the returned stream is bound to the request stream and will auto read/close accordingly

		req
				.writeWith(req
				.log('server-received')
				.map { it + ' ' + req.param('param') + '!' }
				.log('server-reply'))

	}

	then: "the server was started"
	server
	!server.start().get(5, TimeUnit.SECONDS)

	when: "data is sent with Reactor HTTP support"

	//Prepare a client using default impl (Netty) to connect on http://localhost:port/ and assign global codec to send/receive String data
	def client = NetStreams.httpClient {
	  it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
	}

	//prepare an http post request-reply flow
	def content = client.post('/test/World') { HttpChannelFluxion<String, String> req ->
	  //prepare content-type
	  req.header('Content-Type', 'text/plain')

	  //return a producing stream to send some data along the request
	  req.writeWith(Fluxion
			  .just("Hello")
			  .log('client-send'))

	}.flatMap { replies ->
	  //successful request, listen for the first returned next reply and pass it downstream
	  replies
			  .log('client-received')
	}
	.next()
			.doOnError {
	  //something failed during the request or the reply processing
	  println "Failed requesting server: $it"
	}



	then: "data was recieved"
	//the produced reply should be there soon
	content.get(5, TimeUnit.SECONDS) == "Hello World!"

	cleanup: "the client/server where stopped"
	//note how we order first the client then the server shutdown
	client?.shutdown()
	server?.shutdown()
  }

  def "http error with requests from clients"() {
	given: "a simple HttpServer"

	//Listen on localhost using default impl (Netty) and assign a global codec to receive/reply String data
	def server = NetStreams.httpServer {
	  it.httpProcessor(CodecPreprocessor.string()).listen(0)
	}

	when: "the server is prepared"

	CountDownLatch errored = new CountDownLatch(1)

	server.get('/test') { HttpChannelFluxion<String, String> req -> throw new Exception()
	}.get('/test2') { HttpChannelFluxion<String, String> req ->
	  req.writeWith(Fluxion.convert(Observable.error(new Exception()))).flux().log("writeWith").doOnError({
		errored
				.countDown()
	  })
	}.get('/test3') { HttpChannelFluxion<String, String> req -> return Fluxion.error(new Exception())
	}

	then: "the server was started"
	server
	!server.start().get(5, TimeUnit.SECONDS)

	when:
	def client = NetStreams.httpClient {
	  it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
	}

	then:
	server.listenAddress.port



	when: "data is sent with Reactor HTTP support"

	//prepare an http post request-reply flow
	client
			.get('/test')
			.then { replies ->
	 			 Mono.just(replies.responseStatus().code)
			  .log("received-status-1")
			}
			.get(5, TimeUnit.SECONDS)



	then: "data was recieved"
	//the produced reply should be there soon
	thrown HttpException

	when:
	//prepare an http post request-reply flow
	def content = client
			.get('/test2')
			.flatMap { replies -> replies.log("received-status-2")
	}
	.next()
			.get(3, TimeUnit.SECONDS)

	then: "data was recieved"
	//the produced reply should be there soon
	thrown Exceptions.CancelException
	errored.await(5, TimeUnit.SECONDS.SECONDS)
	!content

	when:
	//prepare an http post request-reply flow
	client
			.get('/test3')
			.flatMap { replies ->
	  Fluxion.just(replies.responseStatus().code)
			  .log("received-status-3")
	}
	.next()
			.get(5, TimeUnit.SECONDS)

	then: "data was recieved"
	//the produced reply should be there soon
	thrown HttpException

	cleanup: "the client/server where stopped"
	//note how we order first the client then the server shutdown
	client?.shutdown()
	server?.shutdown()
  }

  def "WebSocket responds to requests from clients"() {
	given: "a simple HttpServer"

	//Listen on localhost using default impl (Netty) and assign a global codec to receive/reply String data
	def server = NetStreams.httpServer {
	  it.httpProcessor(CodecPreprocessor.string()).listen(0)
	}

	def clientRes = 0
	def serverRes = 0

	when: "the server is prepared"

	//prepare websocket request consumer on /test/* and capture the URL parameter "param"
	server
			.get('/test/{param}') {
	  HttpChannelFluxion<String, String> req
		->

		//log then transform then log received http request content from the request body and the resolved URL parameter "param"
		//the returned stream is bound to the request stream and will auto read/close accordingly
		def res = req
				.responseHeader("content-type", "text/plain")
				.writeWith(req.log('server-received')
				.useCapacity(1)
				.doOnNext { serverRes++ }
				.map { it + ' ' + req.param('param') + '!' }
				.log('server-reply'))

		NettyHttpServer.upgradeToWebsocket(req)

		res
	}

	then: "the server was started"
	server
	!server.start().get(5, TimeUnit.SECONDS)

	when: "data is sent with Reactor HTTP support"

	//Prepare a client using default impl (Netty) to connect on http://localhost:port/ and assign global codec to send/receive String data
	def client = NetStreams.httpClient {
	  it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
	}

	//prepare an http websocket request-reply flow
	def content = client.ws('/test/World') { HttpChannelFluxion<String, String> req ->
	  //prepare content-type
	  req.header('Content-Type', 'text/plain')

	  //return a producing stream to send some data along the request
	  req.writeWith(Fluxion
			  .range(1, 1000)
			  .useCapacity(1)
			  .map { it.toString() }
			  .log('client-send'))

	}.flatMap { replies ->
	  //successful handshake, listen for the first returned next replies and pass it downstream
	  replies
			  .log('client-received')
			  .doOnNext { clientRes++ }
	}
	.as(Fluxion.&from)
	.take(1000)
			.toList()
			.doOnError {
	  //something failed during the request or the reply processing
	  println "Failed requesting server: $it"
	}


	println "server: $serverRes / client: $clientRes"

	then: "data was recieved"
	//the produced reply should be there soon
	content.get()[1000 - 1] == "1000 World!"

	cleanup: "the client/server where stopped"
	//note how we order first the client then the server shutdown
	client?.shutdown()
	server?.shutdown()
  }

}