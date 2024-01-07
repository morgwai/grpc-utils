// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.*;

import com.google.common.collect.Comparators;
import io.grpc.stub.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import pl.morgwai.base.grpc.utils.ConcurrentInboundObserver.SubstreamObserver;
import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;
import pl.morgwai.base.utils.concurrent.Awaitable;

import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

import static org.junit.Assert.*;
import static pl.morgwai.base.jul.JulConfigurator.*;



public abstract class ConcurrentInboundObserverTests {



	/** Timeout for standard tests. */
	public static final long TIMEOUT_MILLIS = 500L;

	/** Timeout for slow tests. */
	public static final long SLOW_TIMEOUT_MILLIS = 10_000L;



	FakeOutboundObserver<InboundMessage, OutboundMessage> fakeOutboundObserver;
	LoggingExecutor grpcInternalExecutor;
	Throwable asyncError;

	@Before
	public void setup() {
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 10);
		fakeOutboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
		asyncError = null;
	}



	/** Creates test subject. */
	protected abstract ConcurrentInboundObserver<InboundMessage, OutboundMessage, ?>
	newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, SubstreamObserver<OutboundMessage>> messageHandler,
		BiConsumer<Throwable, ConcurrentInboundObserver<
				InboundMessage, OutboundMessage, ?>> onErrorHandler
	);

	/** See {@link #startMessageDelivery(ConcurrentInboundObserver, InboundMessageProducer)}. */
	protected boolean isClientResponseObserverTest() {
		return false;
	}



	BiConsumer<
			Throwable,
			ConcurrentInboundObserver<InboundMessage, OutboundMessage, ?>>
	interruptThreadErrorHandler(Thread thread) {
		return (error, thisObserver) -> thread.interrupt();
	}



	/**
	 * Simulates client/previous chained call delivering inbound messages. For use with
	 * {@link FakeOutboundObserver#startServerMessageDelivery(StreamObserver, Consumer)} or
	 * {@link FakeOutboundObserver#startClientMessageDelivery(ClientResponseObserver, Consumer)}.
	 * Instances created in test methods.
	 * @see #startMessageDelivery(ConcurrentInboundObserver, InboundMessageProducer)
	 */
	static class InboundMessageProducer implements Consumer<StreamObserver<InboundMessage>> {

		final int numberOfMessages;
		final long maxDeliveryDelayMillis;



		InboundMessageProducer(int numberOfMessages, long maxDeliveryDelayMillis) {
			this.numberOfMessages = numberOfMessages;
			this.maxDeliveryDelayMillis = maxDeliveryDelayMillis;
		}

		InboundMessageProducer(int numberOfMessages) { this(numberOfMessages, 0L); }



		@Override
		public void accept(StreamObserver<InboundMessage> testSubject) {
			// exit if all messages have been already delivered
			if (messageIdSequence >= numberOfMessages) return;

			// deliver the next message immediately or after a slight delay
			if (maxDeliveryDelayMillis > 0L) {
				try {
					Thread.sleep(messageIdSequence % (maxDeliveryDelayMillis + 1L));
				} catch (InterruptedException ignored) {}
			}

			synchronized (deliveryLock) {
				if (messageIdSequence >= numberOfMessages) return;
				final var requestMessage = new InboundMessage(++messageIdSequence);
				if (log.isLoggable(Level.FINER)) log.finer("delivering " + requestMessage);
				testSubject.onNext(requestMessage);
				if (messageIdSequence == numberOfMessages) {
					log.fine("half-closing");
					testSubject.onCompleted();
				}
			}
		}

		volatile int messageIdSequence = 0;
		final Object deliveryLock = new Object(); // ensures requests are delivered in order
	}



	/**
	 * Calls either
	 * {@link FakeOutboundObserver#startServerMessageDelivery(StreamObserver, Consumer)} or
	 * {@link FakeOutboundObserver#startClientMessageDelivery(ClientResponseObserver, Consumer)}
	 * depending on the result from {@link #isClientResponseObserverTest()}.
	 */
	void startMessageDelivery(
		ConcurrentInboundObserver<InboundMessage, OutboundMessage, ?> testSubject,
		InboundMessageProducer inboundMessageProducer
	) {
		if (isClientResponseObserverTest()) {
			fakeOutboundObserver.startClientMessageDelivery(testSubject, inboundMessageProducer);
		} else {
			fakeOutboundObserver.startServerMessageDelivery(testSubject, inboundMessageProducer);
		}
	}



	@Test
	public void testSynchronousProcessingInfiniteBuffer() throws InterruptedException {
		testSynchronousProcessing(50, 0, 0L, TIMEOUT_MILLIS);
	}

	@Test
	public void testSynchronousProcessingBufferOftenUnreadyFor3ms()
		throws InterruptedException {
		testSynchronousProcessing(30, 4, 3L, TIMEOUT_MILLIS);
	}

	@Test
	@Category(SlowTests.class)
	public void testSynchronousProcessingBufferOftenUnreadyFor3ms4kMsgs()
		throws InterruptedException {
		testSynchronousProcessing(4000, 5, 3L, SLOW_TIMEOUT_MILLIS);
	}

	void testSynchronousProcessing(
		int numberOfMessages,
		int outputBufferSize,
		long unreadyDurationMillis,
		long timeoutMillis
	) throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = outputBufferSize;
		fakeOutboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		final int[] onHalfClosedHandlerCallCounterHolder = {0};
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);
		testSubject.setOnHalfClosedHandler(() -> onHalfClosedHandlerCallCounterHolder[0]++);

		startMessageDelivery(testSubject, new InboundMessageProducer(numberOfMessages));
		Awaitable.awaitMultiple(
			timeoutMillis,
			fakeOutboundObserver::awaitFinalization,
			(localTimeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				numberOfMessages, fakeOutboundObserver.getOutputData().size());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
		assertEquals("onHalfClosedHandler should be called once",
				1, onHalfClosedHandlerCallCounterHolder[0]);
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testOnErrorSingleThread() throws Throwable {
		final var numberOfInboundMessages = 2;
		final var error = new Exception("errorToReport");
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				if (inboundMessage.id > 1) {
					asyncError = new Exception("no messages should be requested after the error");
				}
				individualObserver.onError(error);
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(testSubject, new InboundMessageProducer(numberOfInboundMessages));
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination
		);

		assertSame("supplied error should be reported",
				error, fakeOutboundObserver.getReportedError());
		performStandardVerifications(fakeOutboundObserver);
		if (asyncError != null) throw asyncError;
	}



	@Test
	public void testReportErrorAfterTasksAndInboundComplete() throws InterruptedException {
		final var numberOfInboundMessages = 10;
		final var error = new Exception("errorToReport");
		final var testSubject = newConcurrentInboundObserver(
			2,
			(inboundMessage, individualObserver) -> {
				if (inboundMessage.id == 2) {
					individualObserver.reportErrorAfterAllTasksComplete(error);
				}
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(testSubject, new InboundMessageProducer(numberOfInboundMessages));
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				numberOfInboundMessages, fakeOutboundObserver.getOutputData().size());
		assertSame("supplied error should be reported",
				error, fakeOutboundObserver.getReportedError());
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		testSubstreamObserverThrowsOnIllegalOperation(
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
			}
		);
	}

	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		testSubstreamObserverThrowsOnIllegalOperation(
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
				individualObserver.onCompleted();
			}
		);
	}

	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		testSubstreamObserverThrowsOnIllegalOperation(
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
				individualObserver.onError(new Exception());
			}
		);
	}

	void testSubstreamObserverThrowsOnIllegalOperation(
		BiConsumer<InboundMessage,
		CallStreamObserver<OutboundMessage>> messageHandler
	) throws InterruptedException {
		final boolean[] exceptionThrownHolder = { false };
		final var latch = new CountDownLatch(1);
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				try {
					messageHandler.accept(inboundMessage,individualObserver);
				} catch (IllegalStateException expected) {
					exceptionThrownHolder[0] = true;
				} finally {
					latch.countDown();
				}
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(testSubject, new InboundMessageProducer(1));
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			(timeoutMillis) -> latch.await(timeoutMillis, TimeUnit.MILLISECONDS),
			fakeOutboundObserver::awaitFinalization,
			(timeoutMillis) -> {
				grpcInternalExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination
		);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testIndividualOnReadyHandlersAreCalledProperly() throws InterruptedException {
		final var numberOfInboundMessages = 2;
		final int numberOfResultsPerInboundMessage = 3;
		final int[] handlerCallCounters = {0, 0};  // counters per inbound message
		fakeOutboundObserver.outputBufferSize =
				numberOfInboundMessages * numberOfResultsPerInboundMessage - 2;
		final int[] outboundCounters = {0, 0};  // counters per inbound message
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> individualObserver.setOnReadyHandler(() -> {
				final var inboundIndex = inboundMessage.id - 1;
				handlerCallCounters[inboundIndex]++;
				while (
					individualObserver.isReady()
					&& outboundCounters[inboundIndex] < numberOfResultsPerInboundMessage
				) {
					outboundCounters[inboundIndex]++;
					individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				}
				if (outboundCounters[inboundIndex] >= numberOfResultsPerInboundMessage) {
					individualObserver.onCompleted();
				}
			}),
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(testSubject, new InboundMessageProducer(numberOfInboundMessages));
		Awaitable.awaitMultiple(
			TIMEOUT_MILLIS,
			fakeOutboundObserver::awaitFinalization,
			Awaitable.ofTermination(grpcInternalExecutor)
		);

		assertEquals("handler of the first observer should be called once",
				1, handlerCallCounters[0]);
		assertEquals("handler of the second observer should be called twice",
				2, handlerCallCounters[1]);
		assertEquals(
			"correct number of messages should be written",
			numberOfInboundMessages * numberOfResultsPerInboundMessage,
			fakeOutboundObserver.getOutputData().size()
		);
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testAsyncSequentialProcessingBufferOftenUnreadyFor3ms()
			throws InterruptedException {
		testAsyncProcessing(
			20, 3L,
			1, 1, 4L,
			6, 3L,
			TIMEOUT_MILLIS
		);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testAsyncSequentialProcessingBufferVeryOftenUnreadyFor0ms()
		throws InterruptedException {
		testAsyncProcessing(
			50, 1L,
			1, 1, 1L,
			1, 0L,
			TIMEOUT_MILLIS
		);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncSequentialProcessingBufferVeryOftenUnreadyFor0ms2kMsgs()
		throws InterruptedException {
		testAsyncProcessing(
			2000, 1L,
			1, 1, 1L,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms()
			throws InterruptedException {
		testAsyncProcessing(
			100, 3L,
			5, 5, 4L,
			6, 3L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms2kMsgs()
			throws InterruptedException {
		testAsyncProcessing(
			2000, 3L,
			5, 5, 4L,
			6, 3L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads()
			throws InterruptedException {
		testAsyncProcessing(
			100, 3L,
			10, 5, 4L,
			6, 3L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads3kMsgs()
			throws InterruptedException {
		testAsyncProcessing(
			3000, 3L,
			10, 5, 4L,
			6, 3L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays()
			throws InterruptedException {
		testAsyncProcessing(
			100, 0L,
			5, 5, 0L,
			1, 0L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays200kMsgs()
			throws InterruptedException {
		testAsyncProcessing(
			200_000, 0L,
			5, 5, 0L,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms()
			throws InterruptedException {
		testAsyncProcessing(
			100, 3L,
			5, 5, 4L,
			23, 15L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms2kMsgs()
		throws InterruptedException {
		testAsyncProcessing(
			2000, 3L,
			5, 5, 4L,
			23, 15L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads()
		throws InterruptedException {
		testAsyncProcessing(
			100, 3L,
			10, 5, 4L,
			23, 15L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads2kMsgs()
		throws InterruptedException {
		testAsyncProcessing(
			2000, 3L,
			10, 5, 4L,
			23, 15L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	void testAsyncProcessing(
		int numberOfInboundMessages,
		long maxInboundDeliveryDelayMillis,

		int maxConcurrentInboundMessages,
		int numberOfExecutorThreads,
		long maxProcessingDelayMillis,

		int outputBufferSize,
		long unreadyDurationMillis,

		long timeoutMillis
	) throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = outputBufferSize;
		fakeOutboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		final var userExecutor = new LoggingExecutor("userExecutor", numberOfExecutorThreads);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentInboundMessages,
			(inboundMessage, individualObserver) -> {
				final var responseCount = new AtomicInteger(0);
				userExecutor.execute(new Runnable() {

					@Override public void run() {
						simulateProcessingDelay(
							maxProcessingDelayMillis,
							inboundMessage.id + responseCount.get()
						);
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
						individualObserver.onCompleted();
					}

					@Override public String toString() {
						return "task: { inboundMessageId: " + inboundMessage.id + " }";
					}
				});
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(
			testSubject,
			new InboundMessageProducer(numberOfInboundMessages, maxInboundDeliveryDelayMillis)
		);
		Awaitable.awaitMultiple(
			timeoutMillis,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMs) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals("correct number of messages should be written",
				numberOfInboundMessages, fakeOutboundObserver.getOutputData().size());
		userExecutor.verify();
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testOnReadyHandlerIntegrationSequentialSingleTask()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			30, 0L,
			1, 5, 1, 2, 0L,
			4, 3L,
			TIMEOUT_MILLIS
		);
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			20, 3L,
			3, 3, 3, 9, 4L,
			6, 3L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms500msgs()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			500, 3L,
			3, 3, 3, 9, 4L,
			6, 3L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			20, 0L,
			3, 3, 3, 9, 0L,
			1, 0L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public
	void testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays10kMsgs()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			10_000, 0L,
			3, 3, 3, 9, 0L,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test public void
	testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads()
			throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			20, 0L,
			3, 3, 3, 5, 0L,
			1, 0L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void
	testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads10kMsgs
			() throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			10_000, 0L,
			3, 3, 3, 5, 0L,
			1, 0L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	@Test
	public void testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms()
		throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			20, 3L,
			3, 3, 3, 9, 4L,
			40, 15L,
			TIMEOUT_MILLIS
		);
	}

	@Test
	@Category(SlowTests.class)
	public void testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms400msgs()
		throws InterruptedException {
		testDispatchingOnReadyHandlerIntegration(
			400, 3L,
			3, 3, 3, 9, 4L,
			40, 15L,
			SLOW_TIMEOUT_MILLIS
		);
	}

	void testDispatchingOnReadyHandlerIntegration(
		int numberOfInboundMessages,
		long maxInboundDeliveryDelayMillis,

		int handlerTasksPerMessage,
		int resultsPerTask,
		int maxConcurrentInboundMessages,
		int numberOfExecutorThreads,
		long maxProcessingDelayMillis,

		int outputBufferSize,
		long unreadyDurationMillis,

		long timeoutMillis
	) throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = outputBufferSize;
		fakeOutboundObserver.unreadyDurationMillis = unreadyDurationMillis;
		final var userExecutor = new LoggingExecutor("userExecutor", numberOfExecutorThreads);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentInboundMessages,
			(inboundMessage, individualObserver) -> {
				final int[] resultCounters = new int[handlerTasksPerMessage];
				DispatchingOnReadyHandler.copyWithFlowControl(
					individualObserver,
					userExecutor,
					handlerTasksPerMessage,
					(i) -> {
						simulateProcessingDelay(
								maxProcessingDelayMillis, inboundMessage.id + resultCounters[i]);
						resultCounters[i]++;
						return new OutboundMessage(inboundMessage.id);
					},
					(i) -> resultCounters[i] < resultsPerTask
				);
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		startMessageDelivery(
			testSubject,
			new InboundMessageProducer(numberOfInboundMessages, maxInboundDeliveryDelayMillis)
		);
		Awaitable.awaitMultiple(
			timeoutMillis,
			fakeOutboundObserver::awaitFinalization,
			(timeoutMs) -> {
				grpcInternalExecutor.shutdown();
				userExecutor.shutdown();
				return true;
			},
			grpcInternalExecutor::awaitTermination,
			userExecutor::awaitTermination
		);

		assertEquals(
			"correct number of messages should be written",
			numberOfInboundMessages * handlerTasksPerMessage * resultsPerTask,
			fakeOutboundObserver.getOutputData().size()
		);
		userExecutor.verify();
		performStandardVerifications(fakeOutboundObserver);
	}



	public static void performStandardVerifications(FakeOutboundObserver<?, ?> outboundObserver) {
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		assertEquals("no extra finalizations should occur",
				0, outboundObserver.getExtraFinalizationCount());
		assertEquals("no messages should be written after finalization",
				0, outboundObserver.getMessagesAfterFinalizationCount());
		assertTrue("gRPC tasks shouldn't throw exceptions",
				outboundObserver.grpcInternalExecutor.getUncaughtTaskExceptions().isEmpty());
		outboundObserver.grpcInternalExecutor.verify();
	}



	void simulateProcessingDelay(long maxProcessingDelayMillis, long randomNumber) {
		// sleep time varies depending on request/response message counts
		final long processingDelayMillis;
		final var halfOfProcessingDelay = maxProcessingDelayMillis / 2L;
		if (halfOfProcessingDelay > 0L) {
			processingDelayMillis = halfOfProcessingDelay + (randomNumber % halfOfProcessingDelay);
		} else {
			processingDelayMillis = maxProcessingDelayMillis;
		}
		try {
			Thread.sleep(processingDelayMillis);
		} catch (InterruptedException ignored) {}
	}



	static class InboundMessage {

		final int id;

		public InboundMessage(int id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "inbound-" + id;
		}
	}

	static class OutboundMessage {

		final int inboundId;

		public OutboundMessage(int inboundId) {
			this.inboundId = inboundId;
		}

		@Override
		public String toString() {
			return "outbound-" + inboundId;
		}
	}

	static final Comparator<OutboundMessage> outboundMessageComparator =
			Comparator.comparingInt(msg -> msg.inboundId);



	static final Logger log = Logger.getLogger(ConcurrentInboundObserverTests.class.getName());



	/**
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message received/sent and every task dispatched
	 * to {@link LoggingExecutor}.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	@BeforeClass
	public static void setupLogging() {
		addOrReplaceLoggingConfigProperties(Map.of(
			LEVEL_SUFFIX, WARNING.toString(),
			ConsoleHandler.class.getName() + LEVEL_SUFFIX, FINEST.toString()
		));
		overrideLogLevelsWithSystemProperties("pl.morgwai");
	}
}
