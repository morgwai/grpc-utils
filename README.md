# gRPC utils

Some helpful classes when developing gRPC services.<br/>
<br/>
**latest release: [6.0](https://search.maven.org/artifact/pl.morgwai.base/grpc-utils/6.0/jar)**
([javadoc](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/6.0))


## MAIN USER CLASSES

### [ConcurrentInboundObserver](src/main/java/pl/morgwai/base/grpc/utils/ConcurrentInboundObserver.java)
Base class for inbound `StreamObservers` (server method request observers and client response observers), that pass results to some outboundObserver and may dispatch message processing to other threads. Handles all the synchronization and manual flow-control.

### [DispatchingOnReadyHandler](src/main/java/pl/morgwai/base/grpc/utils/DispatchingOnReadyHandler.java)
Streams messages to an outbound `CallStreamObserver` from multiple concurrent tasks with respect to flow-control. Useful when processing of 1 inbound message may result in multiple outbound messages that can be produced concurrently in multiple threads.

### [OrderedConcurrentInboundObserver](src/main/java/pl/morgwai/base/grpc/utils/OrderedConcurrentInboundObserver.java)
A `ConcurrentInboundObserver` that uses [OrderedConcurrentOutputBuffer](https://github.com/morgwai/java-utils/blob/v3.0/src/main/java/pl/morgwai/base/utils/concurrent/OrderedConcurrentOutputBuffer.java) to ensure that outbound messages are sent in the order corresponding to the inbound messages order.

### [BlockingResponseObserver](src/main/java/pl/morgwai/base/grpc/utils/BlockingResponseObserver.java)
A `ClientResponseObserver`, that blocks until the response stream is completed with either `onCompleted()` or `onError(error)`.


## EXAMPLES

See [sample app](sample)
