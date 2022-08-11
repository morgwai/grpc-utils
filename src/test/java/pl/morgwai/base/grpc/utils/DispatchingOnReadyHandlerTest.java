// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import org.junit.*;

import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;

import static org.junit.Assert.*;
import static pl.morgwai.base.grpc.utils.ConcurrentInboundObserverTest.*;



public class DispatchingOnReadyHandlerTest {



	DispatchingOnReadyHandler<Integer> handler;

	FakeOutboundObserver<Integer, ?> outboundObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	LoggingExecutor grpcInternalExecutor;

	/**
	 * Executor passed as handler's constructor param.
	 */
	LoggingExecutor userExecutor;

	int resultCount;
	int[] resultCounters;
	Throwable caughtError;



	@Before
	public void setup() {
		resultCount = 0;
		caughtError = null;
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 5);
		outboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
		userExecutor = new LoggingExecutor("userExecutor", 5);
	}



	@Test
	public void testSingleThread() throws InterruptedException {
		final var numberOfResponses = 10;
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {
				@Override public boolean hasNext() { return resultCount < numberOfResponses; }
				@Override public Integer next() { return ++resultCount; }
			}
		);
		outboundObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(handler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfResponses, outboundObserver.getOutputData().size());
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		assertNull("no exception should be thrown", caughtError);
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testMultiThread() throws InterruptedException {
		final var responsesPerTasks = 5;
		final var numberOfTasks = 5;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			numberOfTasks,
			(i) -> resultCounters[i] >= responsesPerTasks,
			(i) -> ++resultCounters[i],
			Object::toString
		);
		outboundObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(handler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				responsesPerTasks * numberOfTasks,
				outboundObserver.getOutputData().size());
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		assertNull("no exception should be thrown", caughtError);
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testSecondHandlerIsNotSpawned() throws InterruptedException {
		final var numberOfResponses = 10;
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 1l;
		final var concurrencyGuard = new ReentrantLock();
		handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {

				@Override public boolean hasNext() { return resultCount < numberOfResponses; }

				@Override public Integer next() {
					if ( ! concurrencyGuard.tryLock()) {
						final var error =  new AssertionError("another handler detected");
						caughtError = error;  // in case exception handling is also broken
						throw error;
					}
					try {
						Thread.sleep(10l);
					} catch (InterruptedException ignored) {
					} finally {
						concurrencyGuard.unlock();
					}
					return ++resultCount;
				}
			}
		);
		outboundObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(() -> {
			handler.run();
			handler.run();
		});
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertNull("no exception should be thrown", caughtError);
		assertEquals("correct number of messages should be written",
				numberOfResponses, outboundObserver.getOutputData().size());
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testExceptionIsHandledProperly() throws InterruptedException {
		final var numberOfResponses = 5;
		final var thrownException = new RuntimeException();
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5l;
		handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {

				@Override public boolean hasNext() { return resultCount < numberOfResponses; }

				@Override public Integer next() {
					if (resultCount > 2) {
						caughtError = new AssertionError("processing should stop after exception");
					}
					if (resultCount == 2) throw thrownException;
					return ++resultCount;
				}
			}
		);
		outboundObserver.setOnReadyHandler(handler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(handler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("reported exception should be a StatusException",
				outboundObserver.reportedError instanceof StatusException);
		assertSame("reported exception should be a StatusException",
				((StatusException) outboundObserver.reportedError).getStatus().getCode(),
				Code.INTERNAL);
		assertSame("thrownException should be passed to onError as cause",
				thrownException, outboundObserver.reportedError.getCause());
		assertEquals("2 messages should be written",
				2, outboundObserver.getOutputData().size());
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}

	@Test
	public void testErrorIsReportedIfTheLastTaskThrows() throws InterruptedException {
		//todo
	}

	@Test
	public void testErrorIsReportedIfNonFinalTaskThrows() throws InterruptedException {
		//todo
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
		} catch (Exception ignored) {}
		log.setLevel(LOG_LEVEL);
		FakeOutboundObserver.getLogger().setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}
}
