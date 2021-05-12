# gRPC utils

Some helpful classes when developing gRPC services.<br/>
<br/>
**latest release: [1.0-alpha2](https://search.maven.org/artifact/pl.morgwai.base/grpc-utils/1.0-alpha2/jar)**


## MAIN USER CLASSES

### [ConcurrentRequestObserver](src/main/java/pl/morgwai/base/grpc/utils/ConcurrentRequestObserver.java)

A request `StreamObserver` for bi-di streaming methods that dispatch work to other threads and don't need to preserve order of responses. Handles all the synchronization and manual flow control to maintain desired level of concurrency and prevent excessive buffering.


### [OrderedConcurrentOutputBuffer](src/main/java/pl/morgwai/base/utils/OrderedConcurrentOutputBuffer.java)

Buffers messages until all of those that should be written before to the output are available, so that they all can be written in the correct order. Useful for processing input streams in several concurrent threads when order of response messages must reflect the order of request messages.<br/>
(This class comes from [java-utils git submodule](https://github.com/morgwai/java-utils/blob/master/src/main/java/pl/morgwai/base/utils/OrderedConcurrentOutputBuffer.java))


### [BlockingResponseObserver](src/main/java/pl/morgwai/base/grpc/utils/BlockingResponseObserver.java)

A response observer for a client side that blocks until response is completed with either `onCompleted()` or `onError(error)`.


## EXAMPLES

See [sample app for grpc-scopes lib](https://github.com/morgwai/grpc-scopes/tree/master/sample)
