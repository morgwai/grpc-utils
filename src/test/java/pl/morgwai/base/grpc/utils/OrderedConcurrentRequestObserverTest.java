// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Comparators;



public class OrderedConcurrentRequestObserverTest extends ConcurrentRequestObserverTest {



	@Override
	protected ConcurrentRequestObserver<RequestMessage, ResponseMessage>
			newConcurrentRequestObserver() {
		return new OrderedConcurrentRequestObserver<>(
				responseObserver,
				null,
				(error) -> fail("unexpected call"));
	}



	@Override
	public void testAsyncProcessing100Requests5Threads() throws InterruptedException {
		super.testAsyncProcessing100Requests5Threads();
		assertTrue("messages should be written in order",
				Comparators.isInOrder(responseObserver.getOutputData(), responseComparator));
	}



	@Override
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		super.testDispatchingOnReadyHandlerIntegrationMultiThread();
		assertTrue("messages should be written in order",
				Comparators.isInOrder(responseObserver.getOutputData(), responseComparator));
	}
}
