// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Comparators;



public abstract class OrderedConcurrentInboundObserverTest extends ConcurrentInboundObserverTest {



	@Override
	public void testOnErrorMultipleThreads() throws InterruptedException {
		super.testOnErrorMultipleThreads();
		assertTrue("messages should be written in order",
				Comparators.isInOrder(outboundObserver.getOutputData(), outboundMessageComparator));
	}



	@Override
	public void testAsyncProcessingOf100MessagesIn5Threads() throws InterruptedException {
		super.testAsyncProcessingOf100MessagesIn5Threads();
		assertTrue("messages should be written in order",
				Comparators.isInOrder(outboundObserver.getOutputData(), outboundMessageComparator));
	}



	@Override
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		super.testDispatchingOnReadyHandlerIntegrationMultiThread();
		assertTrue("messages should be written in order",
				Comparators.isInOrder(outboundObserver.getOutputData(), outboundMessageComparator));
	}
}
