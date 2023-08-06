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

import org.junit.experimental.categories.Category;
import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;
import pl.morgwai.base.utils.concurrent.Awaitable;

import static org.junit.Assert.*;



public class DispatchingOnReadyHandlerTests {



	/** Timeout for standard tests. */
	public static final long TIMEOUT_MILLIS = 500L;

	/** Timeout for slow tests. */
	public static final long SLOW_TIMEOUT_MILLIS = 10_000L;

	static final String LABEL = "testHandler";
	DispatchingOnReadyHandler<Integer> testHandler;

	FakeOutboundObserver<?, Integer> fakeOutboundObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	LoggingExecutor grpcInternalExecutor;

	/**
	 * Executor passed as handler's constructor param.
	 */
	LoggingExecutor userExecutor;

	int producedMessageCount;
	int[] producedMessageCounters;
	AssertionError asyncAssertionError;



	@Before
	public void setup() {
		producedMessageCount = 0;
		asyncAssertionError = null;
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 5);
		fakeOutboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
		userExecutor = new LoggingExecutor("userExecutor", 10);
	}



	void performStandardVerifications() {
		if (asyncAssertionError != null) throw asyncAssertionError;
		ConcurrentInboundObserverTests.performStandardVerifications(fakeOutboundObserver);
		assertTrue("label should be correctly passed",
				testHandler.toString().contains(LABEL));
	}



	@Test
	public void testSingleThreadBufferOftenUnreadyFor3ms() throws InterruptedException {
		testSingleThread(100, 5, 3L, TIMEOUT_MILLIS);
	}

	@Test
	public void testSingleThreadBufferVeryOftenUnreadyFor0ms() throws InterruptedException {
		testSingleThread(2000, 1, 0L, TIMEOUT_MILLIS);
	}

	@Test
	@Category(SlowTests.class)
	public void testSingleThreadBufferOftenUnreadyFor3ms5kMsgs() throws InterruptedException {
		testSingleThread(5000, 5, 3L, SLOW_TIMEOUT_MILLIS);
	}

	@Test
	@Category(SlowTests.class)
	public void testSingleThreadBufferVeryOftenUnreadyFor0ms200kMsgs() throws InterruptedException {
		testSingleThread(200_000, 1, 0L, SLOW_TIMEOUT_MILLIS);
	}

	public void testSingleThread(
		int numberOfMessages,
		int outputBufferSize,
		long unreadyDurationMillis,
		long timeoutMillis
	) throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = outputBufferSize;
		fakeOutboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		testHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			fakeOutboundObserver,
			userExecutor,
			new Supplier<>() {
				@Override public Integer get() {
					return ++producedMessageCount;
				}
				@Override public String toString() {
					return LABEL;
				}
			},
			() -> producedMessageCount < numberOfMessages
		);

		fakeOutboundObserver.runWithinListenerLock(testHandler);
		Awaitable.awaitMultiple(
			timeoutMillis,
			fakeOutboundObserver::awaitFinalization,
			(localTimeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				numberOfMessages, fakeOutboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		userExecutor.verify();
		performStandardVerifications();
	}



	@Test
	public void testMultiThreadBufferOftenUnreadyFor3ms()
		throws InterruptedException {
		testMultiThread(
			10, 5,
			4, 3L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testMultiThreadBufferOftenUnreadyFor3ms500msgsPerTask()
		throws InterruptedException {
		testMultiThread(
			10, 500,
			4, 3L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testMultiThreadBufferVeryOftenUnreadyFor0ms()
		throws InterruptedException {
		testMultiThread(
			10, 5,
			1, 0L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testMultiThreadBufferVeryOftenUnreadyFor0ms10kMsgsPerTask()
		throws InterruptedException {
		testMultiThread(
			10, 10_000,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testMultiThreadBufferSometimesUnreadyFor15ms()
		throws InterruptedException {
		testMultiThread(
			10, 10,
			17, 15L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testMultiThreadBufferSometimesUnreadyFor15ms500msgsPerTask()
		throws InterruptedException {
		testMultiThread(
			10, 500,
			17, 15L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testMultiThreadBufferVeryOftenUnreadyFor0msTooFewThreads()
		throws InterruptedException {
		testMultiThread(
			20, 5,
			1, 0L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testMultiThreadBufferVeryOftenUnreadyFor0msTooFewThreads10kMsgsPerTask()
		throws InterruptedException {
		testMultiThread(
			20, 10_000,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	public void testMultiThread(
		int numberOfTasks,
		int messagesPerTasks,

		int outputBufferSize,
		long unreadyDurationMillis,

		long timeoutMillis
	) throws InterruptedException {
		producedMessageCounters = new int[numberOfTasks];
		fakeOutboundObserver.outputBufferSize = outputBufferSize;
		fakeOutboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		testHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			fakeOutboundObserver,
			userExecutor,
			numberOfTasks,
			new IntFunction<>() {
				@Override public Integer apply(int numberOfTasks) {
					return ++producedMessageCounters[numberOfTasks];
				}
				@Override public String toString() {
					return LABEL;
				}
			},
			(taskNumber) -> producedMessageCounters[taskNumber] < messagesPerTasks
		);

		fakeOutboundObserver.runWithinListenerLock(testHandler);
		Awaitable.awaitMultiple(
			timeoutMillis,
			fakeOutboundObserver::awaitFinalization,
			(localTimeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				messagesPerTasks * numberOfTasks, fakeOutboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		userExecutor.verify();
		performStandardVerifications();
	}



	@Test
	public void testDuplicateTaskHandlerIsNotSpawned() throws InterruptedException {
		final var numberOfMessages = 10;
		fakeOutboundObserver.outputBufferSize = 3;
		fakeOutboundObserver.unreadyDurationMillis = 1L;
		final var concurrencyGuard = new ReentrantLock();
		testHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			fakeOutboundObserver,
			userExecutor,
			new Iterator<>() {

				@Override public boolean hasNext() {
					return producedMessageCount < numberOfMessages;
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
					return ++producedMessageCount;
				}

				@Override public String toString() {
					return LABEL;
				}
			}
		);

		fakeOutboundObserver.runWithinListenerLock(() -> {
			testHandler.run();
			testHandler.run();
		});
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				numberOfMessages, fakeOutboundObserver.getOutputData().size());
		assertTrue("user tasks shouldn't throw exceptions",
				userExecutor.getUncaughtTaskExceptions().isEmpty());
		userExecutor.verify();
		performStandardVerifications();
	}



	@Test
	public void testReportErrorAfterTasksComplete() throws InterruptedException {
		final StatusException exceptionToReport = Status.INTERNAL.asException();
		final var messagesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		producedMessageCounters = new int[numberOfTasks];
		fakeOutboundObserver.outputBufferSize = 10;
		fakeOutboundObserver.unreadyDurationMillis = 5L;
		testHandler = new DispatchingOnReadyHandler<>(
			fakeOutboundObserver,
			userExecutor,
			numberOfTasks,
			LABEL
		) {

			@Override protected boolean producerHasNext(int taskNumber) {
				return producedMessageCounters[taskNumber] < messagesPerTasks;
			}

			@Override protected Integer produceNextMessage(int taskNumber) {
				if (
					taskNumber == 0
					&& producedMessageCounters[taskNumber] == messageNumberToThrowAfter
				) {
					reportErrorAfterTasksComplete(exceptionToReport);
				}
				return ++producedMessageCounters[taskNumber];
			}
		};
		fakeOutboundObserver.setOnReadyHandler(testHandler);

		fakeOutboundObserver.runWithinListenerLock(testHandler);
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				messagesPerTasks * numberOfTasks, fakeOutboundObserver.getOutputData().size());
		userExecutor.verify();
		assertSame("exceptionToReport should be passed to onError",
				exceptionToReport, fakeOutboundObserver.getReportedError());
		performStandardVerifications();
	}



	@Test
	public void testNoSuchElementExceptionHandling() throws InterruptedException {
		final var messagesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		producedMessageCounters = new int[numberOfTasks];
		fakeOutboundObserver.outputBufferSize = 10;
		fakeOutboundObserver.unreadyDurationMillis = 5L;
		testHandler = DispatchingOnReadyHandler.copyWithFlowControl(
			fakeOutboundObserver,
			userExecutor,
			numberOfTasks,
			(taskNumber) -> {
				if (taskNumber == 0) {
					if (producedMessageCounters[taskNumber] == messageNumberToThrowAfter) {
						throw new NoSuchElementException();
					}
					if (producedMessageCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
								new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++producedMessageCounters[taskNumber];
			},
			(taskNumber) -> producedMessageCounters[taskNumber] < messagesPerTasks,
			LABEL
		);

		fakeOutboundObserver.runWithinListenerLock(testHandler);
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				messagesPerTasks * (numberOfTasks - 1) + messageNumberToThrowAfter,
				fakeOutboundObserver.getOutputData().size());
		userExecutor.verify();
		assertNull("no error should be reported", fakeOutboundObserver.getReportedError());
		performStandardVerifications();
	}



	@Test
	public void testNoSuchElementExceptionAfterReportErrorAfterTasksComplete()
			throws InterruptedException {
		final StatusException exceptionToReport = Status.INTERNAL.asException();
		final var messagesPerTasks = 50;
		final var numberOfTasks = 5;
		final var messageNumberToThrowAfter = 2;
		producedMessageCounters = new int[numberOfTasks];
		fakeOutboundObserver.outputBufferSize = 10;
		fakeOutboundObserver.unreadyDurationMillis = 5L;
		testHandler = new DispatchingOnReadyHandler<>(
			fakeOutboundObserver,
			userExecutor,
			numberOfTasks,
			LABEL
		) {

			@Override protected boolean producerHasNext(int taskNumber) {
				return producedMessageCounters[taskNumber] < messagesPerTasks;
			}

			@Override protected Integer produceNextMessage(int taskNumber) {
				if (taskNumber == 0) {
					if (producedMessageCounters[taskNumber] == messageNumberToThrowAfter) {
						reportErrorAfterTasksComplete(exceptionToReport);
						throw new NoSuchElementException();
					}
					if (producedMessageCounters[taskNumber] >= messageNumberToThrowAfter) {
						asyncAssertionError =
								new AssertionError("processing should stop after exception");
						throw asyncAssertionError;
					}
				}
				return ++producedMessageCounters[taskNumber];
			}
		};
		fakeOutboundObserver.setOnReadyHandler(testHandler);

		fakeOutboundObserver.runWithinListenerLock(testHandler);
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				messagesPerTasks * (numberOfTasks - 1) + messageNumberToThrowAfter,
				fakeOutboundObserver.getOutputData().size());
		userExecutor.verify();
		assertSame("exceptionToReport should be passed to onError",
				exceptionToReport, fakeOutboundObserver.getReportedError());
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
					DispatchingOnReadyHandlerTests.class.getPackageName() + ".level"));
		} catch (Exception ignored) {}
		log.setLevel(LOG_LEVEL);
		FakeOutboundObserver.getLogger().setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}
}
