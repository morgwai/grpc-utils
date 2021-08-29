// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
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
	int[] responseCounters;
	Throwable caughtError;
	int cleanupCount;
	int[] cleanupCounters;



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
	public void testSingleThread() throws InterruptedException {
		final var numberOfexpectedResponses = 10;
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<Integer>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfexpectedResponses,
			() -> ++responseCount,
			(error) -> {
				if (log.isLoggable(Level.FINE)) log.log(Level.FINE, "excp", error);
				caughtError = error;
				synchronized (handler) {
					responseObserver.onError(error);
				}
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(10_000l);
		final var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
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
	public void testMultiThread() throws InterruptedException {
		final var numberOfexpectedResponsesPerThread = 5;
		final var numberOfThreads = 5;
		responseCounters = new int[numberOfThreads];
		cleanupCounters = new int[numberOfThreads];
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<Integer>(
			responseObserver,
			userExecutor,
			numberOfThreads,
			(i) -> responseCounters[i] >= numberOfexpectedResponsesPerThread,
			(i) -> ++responseCounters[i],
			(i, error) -> {
				if (log.isLoggable(Level.FINE)) log.log(Level.FINE, "excp", error);
				caughtError = error;
				synchronized (handler) {
					responseObserver.onError(error);
				}
			},
			(i) -> {
				log.fine("cleanup");
				cleanupCounters[i]++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(10_000l);
		final var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfexpectedResponsesPerThread * numberOfThreads,
				responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertNull("no exception should be thrown", caughtError);
		for (int i = 0; i < numberOfThreads; i++) {
			assertEquals("cleanupHandler should be called 1 time for each thread (" + i + ')',
					1, cleanupCounters[i]);
		}
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
	}



	@Test
	public void testSecondHandlerIsNotSpawned() throws InterruptedException {
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
					final var error =  new AssertionError("another handler detected");
					caughtError = error;  // in case exception handling is also broken
					throw error;
				}
				try {
					Thread.sleep(10l);
				} finally {
					concurrencyGuard.unlock();
				}
				return ++responseCount;
			},
			(error) -> {
				if (log.isLoggable(Level.FINE)) log.log(Level.FINE, "excp", error);
				caughtError = error;
				synchronized (handler) {
					responseObserver.onError(error);
				}
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		responseObserver.runWithinListenerLock(() -> {
			handler.run();
			handler.run();
		});
		responseObserver.awaitFinalization(10_000l);
		final var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertNull("no exception should be thrown", caughtError);
		assertEquals("correct number of messages should be written",
				numberOfexpectedResponses, responseObserver.getOutputData().size());
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
				if (log.isLoggable(Level.FINE)) log.fine("exception " + error);
				caughtError = error;
				synchronized (handler) {
					responseObserver.onError(error);
				}
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(10_000l);
		final var executorShutdownTimeoutMillis = 100l + responseObserver.unreadyDurationMillis;
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



	/**
	 * Change the below value if you need logging:<br/>
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message sent.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static final Level LOG_LEVEL = Level.OFF;

	static final Logger log = Logger.getLogger(DispatchingOnReadyHandler.class.getName());

	@BeforeClass
	public static void setupLogging() {
		var handler = new ConsoleHandler();
		handler.setLevel(LOG_LEVEL);
		log.addHandler(handler);
		log.setLevel(LOG_LEVEL);
		var responseObserverLog = FakeResponseObserver.getLogger();
		responseObserverLog.addHandler(handler);
		responseObserverLog.setLevel(LOG_LEVEL);
	}
}
