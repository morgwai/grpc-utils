// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Comparators;



public abstract class OrderedConcurrentInboundObserverTests extends ConcurrentInboundObserverTests {



	@Override
	public void testAsyncProcessingOf100MessagesIn5Threads() throws InterruptedException {
		super.testAsyncProcessingOf100MessagesIn5Threads();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}



	@Override
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		super.testDispatchingOnReadyHandlerIntegrationMultiThread();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}
}
