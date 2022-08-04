// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



/**
 * TODO
 */
public class OrderedConcurrentRequestObserver<RequestT, ResponseT, OutboundT>
		extends OrderedConcurrentInboundObserver<RequestT, OutboundT> {



	/**
	 * See {@link ConcurrentInboundObserver#ConcurrentInboundObserver(ServerCallStreamObserver, int,
	 * BiConsumer, Consumer) super constructor for param description}.
	 */
	public OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,  // inbound control
		ClientCallStreamObserver<OutboundT> nestedCallRequestObserver,  // outbound
		int numberOfInitialMessages,
		BiConsumer<RequestT, ClientCallStreamObserver<OutboundT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		super(nestedCallRequestObserver, numberOfInitialMessages, requestHandler, errorHandler);
		setInboundControlObserver(responseObserver);
	}



	/**
	 * See {@link
	 * ConcurrentInboundObserver#ConcurrentInboundObserver(ServerCallStreamObserver, int) super}.
	 */
	protected OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,  // inbound control
		ClientCallStreamObserver<OutboundT> nestedCallRequestObserver,  // outbound
		int numberOfInitialMessages
	) {
		super(nestedCallRequestObserver, numberOfInitialMessages);
		setInboundControlObserver(responseObserver);
	}



	public OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<OutboundT> responseObserver,  // outbound + inbound control
		int numberOfInitialMessages,
		BiConsumer<RequestT, CallStreamObserver<OutboundT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		super(responseObserver, numberOfInitialMessages, requestHandler, errorHandler);
		setInboundControlObserver(responseObserver);
	}



	protected OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<OutboundT> responseObserver,  // outbound + inbound control
		int numberOfInitialMessages
	) {
		super(responseObserver, numberOfInitialMessages);
		setInboundControlObserver(responseObserver);
	}
}
