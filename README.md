# reactor-io

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://drone.io/github.com/reactor/reactor-io/status.png)](https://drone.io/github.com/reactor/reactor-io/latest)

Backpressure-ready components to encode, decode, send (unicast, multicast or request/response) and serve connections :
- [reactor-aeron](#reactor-aeron) : Efficient Unicast/Multicast reactive-streams transport for Aeron
- reactor-net   : Client/Server interactions for UDP/TCP/HTTP
- reactor-codec : Reactive-Streams decoders/encoders (Codec) including compression, serialization and such.

## reactor-aeron

An implementation of Reactive Streams over Aeron supporting both unicast and multicast modes of data sending.

### AeronSubscriber + AeronPublisher
A combination of AeronSubscriber playing a role of signals sender and AeronPublisher playing a role of signals receiver allows transporting data from a subscriber to a publisher over Aeron in both unicast and multicast modes.

AeronSubscriber awaiting for connections from AeronPublisher:
```java
AeronSubscriber subscriber = AeronSubscriber.create(new Context()
    .senderChannel("udp://serverbox:12000"));
    
Stream.range(1, 10).map(i -> Buffer.wrap("" + i)).subscribe(subscriber); // sending 1, 2, ..., 10 via Aeron    
```

AeronPublisher connecting to AeronSubscruber above:
```java
AeronPublisher publisher = AeronPublisher.create(new Context()
    .senderChannel("udp://serverbox:12000")     // sender channel specified for AeronSubscriber 
	.receiverChannel("udp://clientbox:12001"));

publisher.subscribe(Subscribers.consumer(System.out::println)); // output: 1, 2, ..., 10
```

### AeronProcessor
A Reactive Streams Processor which plays roles of both signal sender and signal receiver locally and also allows remote instances of AeronPublisher to connect to it via Aeron and request signals.

A processor sending signals via Aeron:
```java
AeronProcessor processor = AeronProcessor.create(new Context()
		.senderChannel("udp://serverbox:12000"));

Stream.range(1, 1000000).map(i -> Buffer.wrap("" + i)).subscribe(processor);

processor.subscribe(Subscribers.consumer(System.out::println));
```

A publisher connecting to the processor above and receiving signals:
```java
AeronPublisher publisher = AeronPublisher.create(new Context()
		.senderChannel("udp://serverbox:12000")
		.receiverChannel("udp://clientbox:12001"));

publisher.subscribe(Subscribers.consumer(System.out::println));
```

## Reference
http://projectreactor.io/io/docs/reference/

## Javadoc
http://projectreactor.io/io/docs/api/

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_

_Sponsored by [Pivotal](http://pivotal.io)_
