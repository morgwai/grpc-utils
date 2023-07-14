// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;

import pl.morgwai.base.grpc.utils.ConcurrentInboundObserver.SubstreamObserver;



public class ConcurrentRequestObserverSendingToNestedTest extends ConcurrentInboundObserverTest {



	@Override protected ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>
	newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, SubstreamObserver<OutboundMessage>> messageHandler,
		BiConsumer<Throwable, ConcurrentInboundObserver<
				InboundMessage, OutboundMessage, OutboundMessage>> onErrorHandler
	) {
		return ConcurrentInboundObserver.newConcurrentServerRequestObserver(
			fakeOutboundObserver.asClientCallRequestObserver(),
			maxConcurrentMessages,
			messageHandler,
			onErrorHandler,
			fakeOutboundObserver.asServerCallResponseObserver()
		);
	}
}
