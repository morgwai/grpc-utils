// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;

import io.grpc.stub.CallStreamObserver;



public class OrderedConcurrentRequestObserverNoNestingTest
		extends OrderedConcurrentInboundObserverTest {



	@Override
	protected ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>
			newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<? super InboundMessage, CallStreamObserver<? super OutboundMessage>>
				messageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<
						? super InboundMessage, ? super OutboundMessage, ? super OutboundMessage>>
				onErrorHandler
	) {
		return new OrderedConcurrentInboundObserver<>(
			outboundObserver.asServerCallResponseObserver(),
			maxConcurrentMessages,
			messageHandler,
			onErrorHandler,
			outboundObserver.asServerCallResponseObserver()
		);
	}
}
