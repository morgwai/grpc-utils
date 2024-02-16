// Copyright 2021 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import io.grpc.*;
import io.grpc.stub.*;



/**
 * A simplified description of gRPC server stub code flow (this class is not included in the jar:
 * it exists only for documentation purposes).
 * <p>
 * In a stub code generated from a proto file in {@link BindableService#bindService()} method, a
 * {@link ServerCallHandler} for each method is created: depending on <b>request</b> type either a
 * {@link ServerCalls.StreamingServerCallHandler} or a
 * {@link ServerCalls.UnaryServerCallHandler}<br/>
 * When a call is received {@link ServerCallHandler#startCall(ServerCall, Metadata)} is called in
 * the interceptor chain, which returns a {@link ServerCall.Listener} specific for the request
 * type.</p>
 * <p><b>Streaming request flow:</b></p>
 * <ol>
 *   <li>When a call is received,
 *     <b>{@link ServerCalls.StreamingServerCallHandler#startCall(ServerCall, Metadata)}</b> calls
 *     {@link ServerCalls.StreamingRequestMethod#invoke(StreamObserver)
 *     StreamingRequestMethod.invoke(responseObserver)} which calls user's method to obtain user's
 *     request {@link StreamObserver}, which is then passed to
 *     {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener
 *     StreamingServerCallListener}'s constructor. Finally, 1 message is requested if
 *     {@code autoRequest} was not disabled (via response
 *     {@link ServerCallStreamObserver#disableAutoRequest()} in the user code),</li>
 *   <li>When a request message arrives, <b>
 *     {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener#onMessage(Object)
 *     StreamingServerCallListener.onMessage(request)}</b> calls user's request observer
 *     {@link StreamObserver#onNext(Object) onNext(request)} to process the message. Then, if
 *     {@code autoRequest} was not disabled, 1 next requests message is requested.
 *     </li>
 *   <li>When the client closes the input stream,
 *     <b>{@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener#onHalfClose()
 *     StreamingServerCallListener.onHalfClose()}</b> calls user's request observer
 *     {@link StreamObserver#onCompleted() onCompleted()}.</li>
 * </ol>
 * <p><b>Unary request flow:</b></p>
 * <ol>
 *   <li>When a call is received,
 *     <b>{@link ServerCalls.UnaryServerCallHandler#startCall(ServerCall, Metadata)}</b> requests
 *     2 messages (only 1 is expected, 2 are requested to trap misbehaving clients) and creates an
 *     {@link ServerCalls.UnaryServerCallHandler.UnaryServerCallListener
 *     UnaryServerCallListener}.</li>
 *   <li>When a request message arrives,
 *     <b>{@link ServerCalls.UnaryServerCallHandler.UnaryServerCallListener#onMessage(Object)
 *     UnaryServerCallListener.onMessage(request)}</b> stores {@code request} and does not call any
 *     user code under normal circumstances.</li>
 *   <li>When the client closes the input stream,
 *     <b>{@link ServerCalls.UnaryServerCallHandler.UnaryServerCallListener#onHalfClose()
 *     UnaryServerCallListener.onHalfClose()}</b> calls
 *     {@link ServerCalls.UnaryRequestMethod#invoke(Object, StreamObserver)
 *     UnaryRequestMethod.invoke(request, responseObserver)}, which calls user's
 *     method passing previously stored {@code request} as an argument.</li>
 * </ol>
 */
public class GrpcServerFlow {}
