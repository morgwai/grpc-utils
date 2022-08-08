// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;

import io.grpc.stub.CallStreamObserver;



public class ConcurrentRequestObserverNoNestingTest
		extends ConcurrentInboundObserverTest {



	@Override
	protected ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>
			newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, CallStreamObserver<OutboundMessage>> messageHandler,
		BiConsumer<Throwable, ConcurrentInboundObserver<
				InboundMessage, OutboundMessage, OutboundMessage>> onErrorHandler
	) {
		return new ConcurrentInboundObserver<>(
			outboundObserver.asServerCallResponseObserver(),
			maxConcurrentMessages,
			messageHandler,
			onErrorHandler,
			outboundObserver.asServerCallResponseObserver()
		);
	}
}
