// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;



/**
 * <b>A <i><u>very</u></i> simplified overview of gRPC server flow:</b><br/>
 * <br/>
 * A {@link ServerCallHandler} for a given method is created, depending on <b>client</b> type either
 * {@link ServerCalls.StreamingServerCallHandler} or {@link ServerCalls.UnaryServerCallHandler}
 * (ie: <code>Streaming</code> / <code>Unary</code> prefix refers to client's type
 * (<code>Server</code> refers to <code>Call</code>), naming scheme here is not intuitive...).<br/>
 * When a call is received {@link ServerCallHandler#startCall(ServerCall, Metadata)} is called in
 * intercepter chain, which returns a {@link ServerCall.Listener}, respectively either
 * {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener} or
 * {@link ServerCalls.UnaryServerCallHandler.UnaryServerCallListener}.<br/>
 * <br/>
 * <br/>
 * <br/>
 * <b>Streaming client flow:</b><br/>
 * <br/>
 * <b>{@link ServerCalls.StreamingServerCallHandler#startCall(ServerCall, Metadata)}</b> calls
 * {@link ServerCalls.StreamingRequestMethod#invoke(StreamObserver)} which calls user's code to
 * obtain request {@link StreamObserver}, which is then passed to constructor of
 * {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener}. Finally, 1 message is
 * requested.<br/>
 * <br/>
 * <b>{@link ServerCall.Listener#onMessage(Object)}</b> calls user's
 * {@link StreamObserver#onNext(Object)} and if <code>autoRequest</code> was not disabled (via
 * response {@link ServerCallStreamObserver#disableAutoRequest()}), it requests 1 next message.<br/>
 * <br/>
 * <b>{@link ServerCall.Listener#onHalfClose()}</b> calls user's
 * {@link StreamObserver#onCompleted()}.<br/>
 * <br/>
 * <br/>
 * <br/>
 * <b>Unary client flow:</b><br/>
 * <br/>
 * <b>{@link ServerCalls.UnaryServerCallHandler#startCall(ServerCall, Metadata)}</b> requests 2
 * messages (only 1 is expected, 2 are requested to trap misbehaving clients).<br/>
 * <br/>
 * <b>{@link ServerCall.Listener#onMessage(Object)}</b> does not call any user code under normal
 * circumstances.<br/>
 * <br/>
 * <b>{@link ServerCall.Listener#onHalfClose()}</b> calls
 * {@link ServerCalls.UnaryRequestMethod#invoke(Object, StreamObserver)} which calls user's code.
 */
public class GrpcServerFlow {}
