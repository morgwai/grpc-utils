# gRPC utils

Some helpful classes when developing gRPC services.<br/>
<br/>
**latest release: [1.0](https://search.maven.org/artifact/pl.morgwai.base/grpc-utils/1.0/jar)** ([javadoc](https://javadoc.io/doc/pl.morgwai.base/grpc-utils/1.0))


## MAIN USER CLASSES

### [DispatchingOnReadyHandler](src/main/java/pl/morgwai/base/grpc/utils/DispatchingOnReadyHandler.java)

Handles streaming messages to a `CallStreamObserver` from multiple threads with respect to flow-control to ensure that no excessive buffering occurs.


### [ConcurrentRequestObserver](src/main/java/pl/morgwai/base/grpc/utils/ConcurrentRequestObserver.java)

A request `StreamObserver` for bi-di streaming methods that dispatch work to multiple threads and don't care about the order of responses. Handles all the synchronization and manual flow control to maintain desired level of concurrency and prevent excessive buffering.


### [OrderedConcurrentRequestObserver](src/main/java/pl/morgwai/base/grpc/utils/OrderedConcurrentRequestObserver.java)

A `ConcurrentRequestObserver` that uses [OrderedConcurrentOutputBuffer](https://github.com/morgwai/java-utils/blob/master/src/main/java/pl/morgwai/base/utils/OrderedConcurrentOutputBuffer.java) to automatically ensure that response messages are sent in order corresponding to request messages order.


### [BlockingResponseObserver](src/main/java/pl/morgwai/base/grpc/utils/BlockingResponseObserver.java)

A response observer for a client side that blocks until response is completed with either `onCompleted()` or `onError(error)`.


## EXAMPLES

See [sample app for grpc-scopes lib](https://github.com/morgwai/grpc-scopes/tree/master/sample)
