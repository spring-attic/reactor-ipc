package reactor.io.net.http

import reactor.io.net.preprocessor.CodecPreprocessor
import reactor.rx.Fluxion
import reactor.rx.net.NetStreams
import reactor.rx.net.http.HttpChannelFluxion
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * @author Anatoly Kadyshev
 */
public class HttpResponseStatusCodesHandlingSpec extends Specification {

    def "http status code 404 is handled by the client"() {
        given: "a simple HttpServer"
            def server = NetStreams.httpServer {
                it.httpProcessor(CodecPreprocessor.string()).listen(0)
            }

        when: "the server is prepared"
            server.post('/test') { HttpChannelFluxion<String, String> req ->
                req.writeWith(
                        req.log('server-received')
                )
            }

        then: "the server was started"
          server
          !server.start().get(Duration.ofSeconds(5))

        when: "a request with unsupported URI is sent onto the server"
            def client = NetStreams.httpClient {
                it.httpProcessor(CodecPreprocessor.string()).connect("localhost", server.listenAddress.port)
            }

            def replyReceived = ""
            def content = client.get('/unsupportedURI') { HttpChannelFluxion<String, String> req ->
                //prepare content-type
                req.header('Content-Type', 'text/plain')

                //return a producing stream to send some data along the request
                req.writeWith(
                    Fluxion
                            .just("Hello")
                            .log('client-send')
                )
            }
            .flatMap { replies ->
                //successful request, listen for replies
                replies
                        .log('client-received')
                        .doOnNext { s ->
                            replyReceived = s
                        }
            }
            .next()
            .doOnError {
                //something failed during the request or the reply processing
                println "Failed requesting server: $it"
            }

        then: "error is thrown with a message and no reply received"
            def exceptionMessage = ""

            try {
                content.get();
            } catch (RuntimeException ex) {
                exceptionMessage = ex.getMessage();
            }

            exceptionMessage == "HTTP request failed with code: 404"
            replyReceived == ""

        cleanup: "the client/server where stopped"
        client?.shutdown()
        server?.shutdown()
    }
}
