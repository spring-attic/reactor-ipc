# reactor-io

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://drone.io/github.com/reactor/reactor-io/status.png)](https://drone.io/github.com/reactor/reactor-io/latest)

Backpressure-ready components to encode, decode, send (unicast, multicast or request/response) and serve connections :
- [reactor-aeron](#reactor-aeron) : Efficient Unicast/Multicast reactive-streams transport for Aeron
- reactor-net   : Client/Server interactions for UDP/TCP/HTTP
- reactor-codec : Reactive-Streams decoders/encoders (Codec) including compression, serialization and such.

## reactor-aeron

An implementation of Reactive Streams over Aeron operating in both unicast and multicast modes.

### AeronSubscriber
A Reactive Streams Subscriber which plays a role of a signals sender via Aeron in unicast/multicast mode to an instance of AeronPublisher.

### AeronPublisher
A Reactive Streams Publisher which plays a role of a signals receiver.

AeronSubscriber and AeronPublisher in action:

On a signals sender side:
```java
AeronSubscriber subscriber = AeronSubscriber.create(new Context()
    .senderChannel("udp://serverbox:12000"));
```

On a signals receiver side:
```java
AeronPublisher publisher = AeronPublisher.create(new Context()
    .senderChannel("udp://serverbox:12000")
    .receiverChannel("udp://clientbox:12001"));
```

### AeronProcessor
A Reactive Streams Processor which plays roles of both a signal sender and a signal receiver locally but also allows
other instances of AeronPublisher to receive signals via Aeron.

## Reference
http://projectreactor.io/io/docs/reference/

## Javadoc
http://projectreactor.io/io/docs/api/

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_
