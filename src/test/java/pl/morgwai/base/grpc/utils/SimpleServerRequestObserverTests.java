// Copyright 2022 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;

import pl.morgwai.base.grpc.utils.ConcurrentInboundObserver.SubstreamObserver;



public class SimpleServerRequestObserverTests extends ConcurrentInboundObserverTests {



	@Override protected ConcurrentInboundObserver<InboundMessage, OutboundMessage, ?>
	newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, SubstreamObserver<OutboundMessage>> messageHandler,
		BiConsumer<Throwable, ConcurrentInboundObserver<
				InboundMessage, OutboundMessage, ?>> onErrorHandler
	) {
		return ConcurrentInboundObserver.newSimpleConcurrentServerRequestObserver(
			fakeOutboundObserver,
			maxConcurrentMessages,
			messageHandler,
			onErrorHandler
		);
	}
}
