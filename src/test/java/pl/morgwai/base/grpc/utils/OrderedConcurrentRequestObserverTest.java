// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertTrue;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.collect.Comparators;
import io.grpc.stub.CallStreamObserver;



public class OrderedConcurrentRequestObserverTest extends ConcurrentRequestObserverTest {



	@Override
	protected ConcurrentRequestObserver<RequestMessage, ResponseMessage>
			newConcurrentRequestObserver(
					int numberOfConcurrentRequests,
					BiConsumer<RequestMessage, CallStreamObserver<ResponseMessage>> requestHandler,
					Consumer<Throwable> errorHandler
	) {
		return new OrderedConcurrentRequestObserver<>(
				responseObserver,
				numberOfConcurrentRequests,
				requestHandler,
				errorHandler);
	}



	@Override
	public void testAsyncProcessingOf100RequestsIn5Threads() throws InterruptedException {
		super.testAsyncProcessingOf100RequestsIn5Threads();
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
