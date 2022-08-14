# Sample app for grpc-utils library

A simple gRPC pipeline processing messages concurrently in several threads and using manual flow-control with utilities from `grpc-utils` lib.


## PIPELINE DESCRIPTION

The pipeline consists of 3 components:
- [BackendServer](src/main/java/pl/morgwai/samples/grpc/utils/BackendServer.java) that implements [Backend service](src/main/proto/backend.proto).
- [SqueezedServer](src/main/java/pl/morgwai/samples/grpc/utils/SqueezedServer.java) that implements [Frontend service](src/main/proto/frontend.proto) and issues nested and chained `Backend` RPCs.
- [Client](src/main/java/pl/morgwai/samples/grpc/utils/Client.java) that issues `Frontend` RPCs

`parent` RPC method of `Frontend` service pre-processes its requests to create requests for `nested` call of `Backend` service. Next, `parent` mid-processes responses from `nested` to create requests for `chained` call of `Backend` service. Finally `parent` post-processes responses from `chained` to create responses for the end client.<br/>
pre-processing, post-processing, `nested` and `chained` create 1 result for each input message. mid-processing creates multiple results per 1 input message (configurable via constructor param and command line argument).


## BUILDING & RUNNING

build: `./mvnw package`

run the whole pipeline with predefined arguments: `java -jar target/grpc-utils-sample-1.0-SNAPSHOT-executable.jar`

Alternatively each component can be started separately with custom param values. See `main` method of each component for details about their params.
