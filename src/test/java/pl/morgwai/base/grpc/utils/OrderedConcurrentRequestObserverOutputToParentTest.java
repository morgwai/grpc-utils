// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;



public class OrderedConcurrentRequestObserverOutputToParentTest
		extends OrderedConcurrentInboundObserverTest {



	protected ConcurrentInboundObserver<InboundMessage, OutboundMessage>
			newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, CallStreamObserver<OutboundMessage>> messageHandler,
		Consumer<Throwable> errorHandler
	) {
		return new OrderedConcurrentRequestObserver<>(
			outboundObserver.asServerCallResponseObserver(),
			maxConcurrentMessages,
			messageHandler,
			errorHandler
		);
	}
}
