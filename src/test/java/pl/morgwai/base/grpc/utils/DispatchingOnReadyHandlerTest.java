// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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



	DispatchingOnReadyHandler<Integer> testSubjectHandler;

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
	AssertionError asyncAssertionError;
	final RuntimeException testException = new RuntimeException("test exception");



	@Before
	public void setup() {
		resultCount = 0;
		asyncAssertionError = null;
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 5);
		outboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
		userExecutor = new LoggingExecutor("userExecutor", 5);
	}



	void performStandardVerifications() {
		ConcurrentInboundObserverTest.performStandardVerifications(outboundObserver);
		assertNull("no assertion should be broken in other threads", asyncAssertionError);
	}



	@Test
	public void testSingleThread() throws InterruptedException {
		final var numberOfResponses = 10;
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {
				@Override public boolean hasNext() { return resultCount < numberOfResponses; }
				@Override public Integer next() { return ++resultCount; }
			}
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfResponses, outboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		verifyExecutor(userExecutor);
		performStandardVerifications();
	}



	@Test
	public void testMultiThread() throws InterruptedException {
		final var responsesPerTasks = 5;
		final var numberOfTasks = 5;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			numberOfTasks,
			(i) -> resultCounters[i] < responsesPerTasks,
			(i) -> ++resultCounters[i],
			Object::toString
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				responsesPerTasks * numberOfTasks, outboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		verifyExecutor(userExecutor);
		performStandardVerifications();
	}



	@Test
	public void testDuplicateTaskHandlerIsNotSpawned() throws InterruptedException {
		final var numberOfResponses = 10;
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 1L;
		final var concurrencyGuard = new ReentrantLock();
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {

				@Override public boolean hasNext() { return resultCount < numberOfResponses; }

				@Override public Integer next() {
					if ( ! concurrencyGuard.tryLock()) {
						final var error =  new AssertionError("another handler detected");
						asyncAssertionError = error;  // in case exception handling is also broken
						throw error;
					}
					try {
						Thread.sleep(10L);
					} catch (InterruptedException ignored) {
					} finally {
						concurrencyGuard.unlock();
					}
					return ++resultCount;
				}
			}
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(() -> {
			testSubjectHandler.run();
			testSubjectHandler.run();
		});
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfResponses, outboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		verifyExecutor(userExecutor);
		performStandardVerifications();
	}



	void performExceptionHandlingVerifications() {
		verifyExecutor(userExecutor, testException);
		assertNotNull("exception should be reported via onError()", outboundObserver.reportedError);
		assertTrue("reported exception should be a StatusException",
				outboundObserver.reportedError instanceof StatusException);
		assertSame("Status should have code INTERNAL",
				((StatusException) outboundObserver.reportedError).getStatus().getCode(),
				Code.INTERNAL);
		assertSame("testException should be passed to onError as the cause",
				testException, outboundObserver.reportedError.getCause());
	}



	@Test
	public void testExceptionIsReportedToOutboundObserverAndTaskIsAborted()
			throws InterruptedException {
		final var numberOfResponses = 5;
		final var messageNumberToThrowAfter = 2;
		outboundObserver.outputBufferSize = 2;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			new Iterator<>() {

				@Override public boolean hasNext() { return resultCount < numberOfResponses; }

				@Override public Integer next() {
					if (resultCount == messageNumberToThrowAfter) throw testException;
					if (resultCount >= messageNumberToThrowAfter) {
						asyncAssertionError =
								new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
					return ++resultCount;
				}
			}
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("2 messages should be written",
				messageNumberToThrowAfter, outboundObserver.getOutputData().size());
		performExceptionHandlingVerifications();
		performStandardVerifications();
	}



	@Test
	public void testOtherTasksAreCancelledAfterErrorIsReported() throws InterruptedException {
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			false,
			numberOfTasks,
			(taskNumber) -> resultCounters[taskNumber] < responsesPerTasks,
			(taskNumber) -> {
				if (taskNumber == 0) {
					if (resultCounters[taskNumber] == messageNumberToThrowAfter) {
						throw testException;
					}
					if (resultCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
								new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++resultCounters[taskNumber];
			},
			Object::toString
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("not all messages should be written",
				responsesPerTasks * (numberOfTasks - 1) + messageNumberToThrowAfter
				> outboundObserver.getOutputData().size());
		assertTrue("not all tasks should complete",
				numberOfTasks > testSubjectHandler.completedTaskCount);
		performExceptionHandlingVerifications();
		performStandardVerifications();
	}



	@Test
	public void testExceptionIsReportedAndOtherTasksCompleteIfTheLastUncompletedTaskThrows()
			throws InterruptedException {
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 10;
		final var otherTasksCompleted = new CountDownLatch(numberOfTasks - 1);
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		final var startMillis = System.currentTimeMillis();
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			true,
			numberOfTasks,
			(taskNumber) -> resultCounters[taskNumber] < responsesPerTasks,
			(taskNumber) -> {
				if (taskNumber == 0) {
					if (resultCounters[taskNumber] == messageNumberToThrowAfter) {
						try {
							otherTasksCompleted.await(
									getRemainingMillis(startMillis), TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
							asyncAssertionError =
									new AssertionError("other tasks failed to complete");
							throw asyncAssertionError;
						}
						throw testException;
					}
					if (resultCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
							new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}

				++resultCounters[taskNumber];
				if (resultCounters[taskNumber] == responsesPerTasks) {
					otherTasksCompleted.countDown();
				}
				return  resultCounters[taskNumber];
			},
			Object::toString
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				responsesPerTasks * (numberOfTasks - 1) + messageNumberToThrowAfter,
				outboundObserver.getOutputData().size());
		performExceptionHandlingVerifications();
		performStandardVerifications();
	}



	@Test
	public void testExceptionIsReportedAndOtherTasksCompleteIfNonFinalTaskThrows()
			throws InterruptedException {
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			true,
			numberOfTasks,
			(taskNumber) -> resultCounters[taskNumber] < responsesPerTasks,
			(taskNumber) -> {
				if (taskNumber == 0) {
					if (resultCounters[taskNumber] == messageNumberToThrowAfter) {
						throw testException;
					}
					if (resultCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
							new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++resultCounters[taskNumber];
			},
			Object::toString
		);
		outboundObserver.setOnReadyHandler(testSubjectHandler);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.runWithinListenerLock(testSubjectHandler);
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				responsesPerTasks * (numberOfTasks - 1) + messageNumberToThrowAfter,
				outboundObserver.getOutputData().size());
		performExceptionHandlingVerifications();
		performStandardVerifications();
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
