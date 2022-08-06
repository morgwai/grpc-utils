// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;



public class ConcurrentInboundObserverSendingToNestedFromParentTest
		extends ConcurrentInboundObserverTest {



	@Override
	protected ConcurrentInboundObserver<InboundMessage, OutboundMessage, ?>
			newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, CallStreamObserver<OutboundMessage>> messageHandler,
		Consumer<Throwable> errorHandler
	) {
		return new ConcurrentInboundObserverSendingToNested<>(
			outboundObserver.asClientCallRequestObserver(),
			maxConcurrentMessages,
			messageHandler::accept,
			errorHandler,
			outboundObserver.asServerCallResponseObserver()
		);
	}
}
