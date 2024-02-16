# gRPC utils

Some helpful classes when developing gRPC services.<br/>
Copyright 2021 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0<br/>
<br/>
**latest release: [7.0](https://search.maven.org/artifact/pl.morgwai.base/grpc-utils/7.0/jar)**
([javadoc](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/7.0))


## MAIN USER CLASSES

### [ConcurrentInboundObserver](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/latest/pl/morgwai/base/grpc/utils/ConcurrentInboundObserver.html)
Base class for inbound `StreamObservers` (server method request observers and client response observers), that pass results to some outboundObserver and may dispatch message processing to other threads. Handles all the synchronization and manual flow-control.

### [DispatchingOnReadyHandler](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/latest/pl/morgwai/base/grpc/utils/DispatchingOnReadyHandler.html)
Streams messages to an outbound `CallStreamObserver` from multiple concurrent tasks with respect to flow-control. Useful when processing of 1 inbound message may result in multiple outbound messages that can be produced concurrently in multiple threads.

### [OrderedConcurrentInboundObserver](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/latest/pl/morgwai/base/grpc/utils/OrderedConcurrentInboundObserver.html)
A `ConcurrentInboundObserver` that uses [OrderedConcurrentOutputBuffer](https://javadoc.io/doc/pl.morgwai.base/java-utils/latest/pl/morgwai/base/utils/concurrent/OrderedConcurrentOutputBuffer.html) to ensure that outbound messages are sent in the order corresponding to the inbound messages order.

### [BlockingResponseObserver](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/latest/pl/morgwai/base/grpc/utils/BlockingResponseObserver.html)
A `ClientResponseObserver`, that blocks until the response stream is completed with either `onCompleted()` or `onError(error)`.


## EXAMPLES

See [sample app](sample)
