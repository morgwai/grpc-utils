// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import io.grpc.*;
import io.grpc.stub.*;



/**
 * A <i><u>very</u></i> simplified overview of gRPC server flow.
 * <p>
 * A {@link ServerCallHandler} for a given method is created, depending on <b>client</b> type either
 * {@link ServerCalls.StreamingServerCallHandler} or {@link ServerCalls.UnaryServerCallHandler}
 * (ie: <code>Streaming</code> / <code>Unary</code> prefix refers to client's type
 * (<code>Server</code> refers to <code>Call</code>), naming scheme here is not intuitive...).<br/>
 * When a call is received {@link ServerCallHandler#startCall(ServerCall, Metadata)} is called in
 * the interceptor chain, which returns a {@link ServerCall.Listener}, respectively either
 * {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener} or
 * {@link ServerCalls.UnaryServerCallHandler.UnaryServerCallListener}.</p>
 * <p>
 * <b>Streaming client flow:</b><ul>
 *   <li><b>{@link ServerCalls.StreamingServerCallHandler#startCall(ServerCall, Metadata)}</b> calls
 *     {@link ServerCalls.StreamingRequestMethod#invoke(StreamObserver)} which calls user's method
 *     to obtain request {@link StreamObserver}, which is then passed to constructor of
 *     {@link ServerCalls.StreamingServerCallHandler.StreamingServerCallListener}. Finally, 1
 *     message is requested.</li>
 *   <li><b>{@link ServerCall.Listener#onMessage(Object)}</b> calls user's
 *     {@link StreamObserver#onNext(Object)} and if <code>autoRequest</code> was not disabled (via
 *     response {@link ServerCallStreamObserver#disableAutoRequest()}), it requests 1 next message.
 *     </li>
 *   <li><b>{@link ServerCall.Listener#onHalfClose()}</b> calls user's
 *     {@link StreamObserver#onCompleted()}.</li>
 * </ul></p>
 * <p>
 * <b>Unary client flow:</b><ul>
 *   <li><b>{@link ServerCalls.UnaryServerCallHandler#startCall(ServerCall, Metadata)}</b> requests
 *     2 messages (only 1 is expected, 2 are requested to trap misbehaving clients).</li>
 *   <li><b>{@link ServerCall.Listener#onMessage(Object)}</b> does not call any user code under
 *     normal circumstances.</li>
 *   <li><b>{@link ServerCall.Listener#onHalfClose()}</b> calls
 *     {@link ServerCalls.UnaryRequestMethod#invoke(Object, StreamObserver)} which calls user's
 *     method.</li>
 * </ul></p>
 */
public class GrpcServerFlow {}
