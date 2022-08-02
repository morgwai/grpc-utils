// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



// TODO javadocs
public class ConcurrentResponseObserver<RequestT, ResponseT, OutboundT>
		extends ConcurrentInboundObserver<ResponseT, OutboundT>
		implements ClientResponseObserver<RequestT, ResponseT> {



	protected void onPreStart(ClientCallStreamObserver<RequestT> callRequestObserver) {
		preStartHandler.accept(callRequestObserver);
	}

	protected Consumer<ClientCallStreamObserver<RequestT>> preStartHandler;



	public ConcurrentResponseObserver(
		ServerCallStreamObserver<OutboundT> parentCallResponseObserver,
		int numberOfInitiallyRequestedMessages,
		BiConsumer<ResponseT, CallStreamObserver<OutboundT>> responseHandler,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<RequestT>> preStartHandler
	) {
		super(
			parentCallResponseObserver,
			numberOfInitiallyRequestedMessages,
			responseHandler,
			errorHandler
		);
		this.preStartHandler = preStartHandler;
	}



	protected ConcurrentResponseObserver(
		ServerCallStreamObserver<OutboundT> parentCallResponseObserver,
		int numberOfInitiallyRequestedMessages
	) {
		super(parentCallResponseObserver, numberOfInitiallyRequestedMessages);
	}



	public ConcurrentResponseObserver(
		ClientCallStreamObserver<OutboundT> chainedCallRequestObserver,
		int numberOfInitiallyRequestedMessages,
		BiConsumer<ResponseT, CallStreamObserver<OutboundT>> responseHandler,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<RequestT>> preStartHandler
	) {
		super(
			chainedCallRequestObserver,
			numberOfInitiallyRequestedMessages,
			responseHandler,
			errorHandler
		);
		this.preStartHandler = preStartHandler;
	}



	protected ConcurrentResponseObserver(
		ClientCallStreamObserver<OutboundT> chainedCallRequestObserver,
		int numberOfInitiallyRequestedMessages
	) {
		super(chainedCallRequestObserver, numberOfInitiallyRequestedMessages);
	}



	@Override
	public final void beforeStart(ClientCallStreamObserver<RequestT> callRequestObserver)
	{
		setInboundControlObserver(callRequestObserver);
		onPreStart(callRequestObserver);
	}
}
