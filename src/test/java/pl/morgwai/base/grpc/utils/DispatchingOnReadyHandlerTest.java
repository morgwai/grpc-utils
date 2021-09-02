// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import pl.morgwai.base.grpc.utils.FakeResponseObserver.LoggingExecutor;

import static org.junit.Assert.*;
import static pl.morgwai.base.grpc.utils.ConcurrentRequestObserverTest.*;



public class DispatchingOnReadyHandlerTest {



	DispatchingOnReadyHandler<Integer> handler;

	FakeResponseObserver<Integer> responseObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	LoggingExecutor grpcInternalExecutor;

	/**
	 * Executor passed as handler's constructor param.
	 */
	LoggingExecutor userExecutor;

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
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 5);
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
		userExecutor = new LoggingExecutor("userExecutor", 5);
	}



	@Test
	public void testSingleThread() throws InterruptedException {
		final var numberOfResponses = 10;
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<Integer>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfResponses,
			() -> ++responseCount,
			(error) -> {
				if (log.isLoggable(Level.FINE)) log.log(Level.FINE, "excp", error);
				caughtError = error;
				return error;
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfResponses, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertNull("no exception should be thrown", caughtError);
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testMultiThread() throws InterruptedException {
		final var responsesPerTasks = 5;
		final var numberOfTasks = 5;
		responseCounters = new int[numberOfTasks];
		cleanupCounters = new int[numberOfTasks];
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<Integer>(
			responseObserver,
			userExecutor,
			numberOfTasks,
			(i) -> responseCounters[i] >= responsesPerTasks,
			(i) -> ++responseCounters[i],
			(i, error) -> {
				if (log.isLoggable(Level.FINE)) log.log(Level.FINE, "excp", error);
				caughtError = error;
				return error;
			},
			(i) -> {
				log.fine("cleanup");
				cleanupCounters[i]++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				responsesPerTasks * numberOfTasks,
				responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertNull("no exception should be thrown", caughtError);
		for (int i = 0; i < numberOfTasks; i++) {
			assertEquals("cleanupHandler should be called 1 time for each thread (" + i + ')',
					1, cleanupCounters[i]);
		}
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testSecondHandlerIsNotSpawned() throws InterruptedException {
		final var numberOfResponses = 10;
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 1l;
		final var concurrencyGuard = new ReentrantLock();
		handler = new DispatchingOnReadyHandler<>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfResponses,
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
				return error;
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		responseObserver.runWithinListenerLock(() -> {
			handler.run();
			handler.run();
		});
		responseObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertNull("no exception should be thrown", caughtError);
		assertEquals("correct number of messages should be written",
				numberOfResponses, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testExceptionIsHandledProperly() throws InterruptedException {
		final var numberOfResponses = 5;
		final var thrownException = new Exception();
		responseObserver.outputBufferSize = 3;
		responseObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<>(
			responseObserver,
			userExecutor,
			() -> responseCount >= numberOfResponses,
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
				return error;
			},
			() -> {
				log.fine("cleanup");
				cleanupCount++;
			}
		);
		responseObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		responseObserver.runWithinListenerLock(handler);
		responseObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertSame("thrownException should be passed to exceptionHandler",
				thrownException, caughtError);
		assertSame("thrownException should be passed to onError",
				thrownException, responseObserver.reportedError);
		assertEquals("2 messages should be written",
				2, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertEquals("cleanupHandler should be called 1 time", 1, cleanupCount);
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	/**
	 * Change the below value if you need logging:<br/>
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message received/sent and every task dispatched
	 * to {@link LoggingExecutor}.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static Level LOG_LEVEL = Level.WARNING;

	static final Logger log = Logger.getLogger(DispatchingOnReadyHandler.class.getName());

	@BeforeClass
	public static void setupLogging() {
		try {
			LOG_LEVEL = Level.parse(System.getProperty(
					DispatchingOnReadyHandlerTest.class.getPackageName() + ".level"));
		} catch (Exception e) {}
		log.setLevel(LOG_LEVEL);
		FakeResponseObserver.getLogger().setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}
}
