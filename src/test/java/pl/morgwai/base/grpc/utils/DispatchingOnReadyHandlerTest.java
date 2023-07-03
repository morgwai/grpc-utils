// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.*;
import org.junit.*;

import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;

import static org.junit.Assert.*;
import static pl.morgwai.base.grpc.utils.ConcurrentInboundObserverTest.*;



public class DispatchingOnReadyHandlerTest {



	static final String LABEL = "testSubjectHandler";
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
		assertTrue("label should be correctly passed",
				testSubjectHandler.toString().contains(LABEL));
	}



	public void testSingleThread(
		int numberOfResponses,
		int outputBufferSize,
		long unreadyDurationMillis
	) throws InterruptedException {
		outboundObserver.outputBufferSize = outputBufferSize;
		outboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		testSubjectHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			outboundObserver,
			userExecutor,
			() -> resultCount < numberOfResponses,
			new Supplier<>() {
				@Override public Integer get() {
					return ++resultCount;
				}
				@Override public String toString() {
					return LABEL;
				}
			}
		);

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
	public void testSingleThreadObserverSometimesUnreadyForFewMs() throws InterruptedException {
		testSingleThread(100, 5, 3L);
	}

	@Test
	public void testSingleThreadObserverOftenUnreadyForSplitMs() throws InterruptedException {
		testSingleThread(2000, 1, 0L);
	}



	@Test
	public void testMultiThread() throws InterruptedException {
		final var responsesPerTasks = 5;
		final var numberOfTasks = 5;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 3;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			outboundObserver,
			userExecutor,
			numberOfTasks,
			(taskNumber) -> resultCounters[taskNumber] < responsesPerTasks,
			new IntFunction<>() {
				@Override public Integer apply(int numberOfTasks) {
					return ++resultCounters[numberOfTasks];
				}
				@Override public String toString() {
					return LABEL;
				}
			}
		);

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
		testSubjectHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			outboundObserver,
			userExecutor,
			new Iterator<>() {

				@Override public boolean hasNext() {
					return resultCount < numberOfResponses;
				}

				@Override public Integer next() {
					if ( !concurrencyGuard.tryLock()) {
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

				@Override public String toString() {
					return LABEL;
				}
			}
		);

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



	@Test
	public void testReportErrorAfterTasksComplete() throws InterruptedException {
		final StatusException exceptionToReport = Status.INTERNAL.asException();
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			numberOfTasks,
			LABEL
		) {

			@Override protected boolean producerHasNext(int taskNumber) {
				return resultCounters[taskNumber] < responsesPerTasks;
			}

			@Override protected Integer produceNextMessage(int taskNumber) {
				if (taskNumber == 0 && resultCounters[taskNumber] == messageNumberToThrowAfter) {
					reportErrorAfterTasksComplete(exceptionToReport);
				}
				return ++resultCounters[taskNumber];
			}
		};
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
		verifyExecutor(userExecutor);
		assertSame("exceptionToReport should be passed to onError",
				exceptionToReport, outboundObserver.reportedError);
		performStandardVerifications();
	}



	@Test
	public void testNoSuchElementExceptionHandling() throws InterruptedException {
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			outboundObserver,
			userExecutor,
			numberOfTasks,
			(taskNumber) -> resultCounters[taskNumber] < responsesPerTasks,
			(taskNumber) -> {
				if (taskNumber == 0) {
					if (resultCounters[taskNumber] == messageNumberToThrowAfter) {
						throw new NoSuchElementException();
					}
					if (resultCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
							new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++resultCounters[taskNumber];
			},
			LABEL
		);

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
		verifyExecutor(userExecutor);
		assertNull("no error should be reported", outboundObserver.reportedError);
		performStandardVerifications();
	}



	@Test
	public void testNoSuchElementExceptionAfterReportErrorAfterTasksComplete()
			throws InterruptedException {
		final StatusException exceptionToReport = Status.INTERNAL.asException();
		final var responsesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		resultCounters = new int[numberOfTasks];
		outboundObserver.outputBufferSize = 10;
		outboundObserver.unreadyDurationMillis = 5L;
		testSubjectHandler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			userExecutor,
			numberOfTasks,
			LABEL
		) {

			@Override protected boolean producerHasNext(int taskNumber) {
				return resultCounters[taskNumber] < responsesPerTasks;
			}

			@Override protected Integer produceNextMessage(int taskNumber) {
				if (taskNumber == 0) {
					if (resultCounters[taskNumber] == messageNumberToThrowAfter) {
						reportErrorAfterTasksComplete(exceptionToReport);
						throw new NoSuchElementException();
					}
					if (resultCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
							new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++resultCounters[taskNumber];
			}
		};
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
		verifyExecutor(userExecutor);
		assertSame("exceptionToReport should be passed to onError",
				exceptionToReport, outboundObserver.reportedError);
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
