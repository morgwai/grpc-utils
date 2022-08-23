# gRPC utils

Some helpful classes when developing gRPC services.<br/>
<br/>
**latest release: [3.3](https://search.maven.org/artifact/pl.morgwai.base/grpc-utils/3.3/jar)**
([javadoc](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/3.3))


## MAIN USER CLASSES

### [DispatchingOnReadyHandler](src/main/java/pl/morgwai/base/grpc/utils/DispatchingOnReadyHandler.java)
Streams messages to an outbound `CallStreamObserver` from multiple sources in separate threads with respect to flow-control. Useful in sever methods when 1 request message can result in multiple response messages that can be produced concurrently in separate tasks.

### [ConcurrentInboundObserver](src/main/java/pl/morgwai/base/grpc/utils/ConcurrentInboundObserver.java)
Base class for inbound `StreamObserver`s (server request observers for RPC method implementations and client response observers for nested or chained calls), that may dispatch message processing to multiple threads: handles all the synchronization and manual flow-control.

### [OrderedConcurrentInboundObserver](src/main/java/pl/morgwai/base/grpc/utils/OrderedConcurrentInboundObserver.java)
A `ConcurrentInboundObserver` that uses [OrderedConcurrentOutputBuffer](https://github.com/morgwai/java-utils/blob/master/src/main/java/pl/morgwai/base/concurrent/OrderedConcurrentOutputBuffer.java) to ensure that outbound messages are sent in the order corresponding to the inbound messages order.

### [BlockingResponseObserver](src/main/java/pl/morgwai/base/grpc/utils/BlockingResponseObserver.java)
A `ClientResponseObserver`, that blocks until response is completed with either `onCompleted()` or `onError(error)`.


## EXAMPLES

See [sample app](sample)
