// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Test;

import pl.morgwai.base.grpc.utils.FakeResponseObserver.FailureTrackingThreadPoolExecutor;

import static org.junit.Assert.*;



public class DispatchingOnReadyHandlerTest {



	DispatchingOnReadyHandler<Integer> handler;

	FakeResponseObserver<Integer> responseObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	FailureTrackingThreadPoolExecutor grpcInternalExecutor;

	/**
	 * Executor passed as handler's constructor param.
	 */
	FailureTrackingThreadPoolExecutor userExecutor;

	int responseCount;
	Throwable caughtError;
	int cleanupCount;



	@Before
	public void setup() {
		responseCount = 0;
		caughtError = null;
		cleanupCount = 0;
		grpcInternalExecutor = new FailureTrackingThreadPoolExecutor(5);
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
		userExecutor = new FailureTrackingThreadPoolExecutor(5);
	}



	@Test
	public void testPositiveCase() throws InterruptedException {
		final var numberOfexpectedResponses = 10;
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<Integer>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfexpectedResponses,
			() -> ++responseCount,
			(error) -> {
				caughtError = error;
				responseObserver.onError(error);
			},
			() -> cleanupCount++
		);
		responseObserver.setOnReadyHandler(handler);

		handler.run();
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("all messages should be written",
				numberOfexpectedResponses, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertNull("no exception should be thrown", caughtError);
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
	}



	@Test
	public void testSecondHandlerwillNotSpawn() throws InterruptedException {
		final var numberOfexpectedResponses = 10;
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 1l;
		final var concurrencyGuard = new ReentrantLock();
		handler = new DispatchingOnReadyHandler<>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfexpectedResponses,
			() -> {
				if ( ! concurrencyGuard.tryLock()) {
					caughtError = new AssertionError("another handler detected");
				}
				try {
					Thread.sleep(5);
				} finally {
					concurrencyGuard.unlock();
				}
				return ++responseCount;
			},
			(error) -> {
				caughtError = error;
				responseObserver.onError(error);
			},
			() -> cleanupCount++
		);
		responseObserver.setOnReadyHandler(handler);

		handler.run();
		handler.run();
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("all messages should be written",
				numberOfexpectedResponses, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertNull("no exception should be thrown", caughtError);
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
	}



	@Test
	public void testExceptionIsHandledProperly() throws InterruptedException {
		final var numberOfexpectedResponses = 5;
		final var thrownException = new FileNotFoundException();
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfexpectedResponses,
			() -> {
				if (responseCount > 2) {
					caughtError = new AssertionError("processing should stop after exception");
				}
				if (responseCount == 2) throw thrownException;
				return ++responseCount;
			},
			(error) -> {
				caughtError = error;
				responseObserver.onError(error);
			},
			() -> cleanupCount++
		);
		responseObserver.setOnReadyHandler(handler);

		handler.run();
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertSame("FileNotFoundException should be thrown", thrownException, caughtError);
		assertEquals("2 messages should be written",
				2, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
	}
}
