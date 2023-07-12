// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;
import pl.morgwai.base.util.concurrent.Awaitable;

import static org.junit.Assert.*;



public abstract class ConcurrentInboundObserverTest {



	/** Timeout for single-threaded, no-processing-delay operations. */
	public static final long TIMEOUT_MILLIS = 500L;



	FakeOutboundObserver<OutboundMessage, Integer> fakeOutboundObserver;
	LoggingExecutor grpcInternalExecutor;
	Throwable asyncError;

	@Before
	public void setup() {
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 10);
		fakeOutboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
		asyncError = null;
	}



	/**
	 * Creates test subject.
	 */
	protected abstract ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>
	newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, CallStreamObserver<OutboundMessage>> messageHandler,
		BiConsumer<Throwable, ConcurrentInboundObserver<
				InboundMessage, OutboundMessage, OutboundMessage>> onErrorHandler
	);

	BiConsumer<
			Throwable,
			ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>>
	interruptThreadErrorHandler(Thread thread) {
		return (error, thisObserver) -> thread.interrupt();
	}



	/**
	 * Simulates client/previous chained call delivering inbound messages.
	 * For use with {@link FakeOutboundObserver#startMessageDelivery(StreamObserver, Consumer)}.
	 * Instances created in test methods.
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



	void testSynchronousProcessing(int numberOfMessages) throws InterruptedException {
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		fakeOutboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
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
				numberOfMessages, fakeOutboundObserver.getOutputData().size());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
		performStandardVerifications(fakeOutboundObserver);
	}

	@Test
	public void testSynchronousProcessingOutboundObserverAlwaysReady() throws InterruptedException {
		testSynchronousProcessing(10);
	}

	@Test
	public void testSynchronousProcessingOutboundObserverUnreadySometimes()
			throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = 4;
		fakeOutboundObserver.unreadyDurationMillis = 3L;
		testSynchronousProcessing(15);
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

		fakeOutboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfInboundMessages));
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
		@SuppressWarnings("unchecked")
		final ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>[] holder =
				new ConcurrentInboundObserver[1];
		final var testSubject = newConcurrentInboundObserver(
			2,
			(inboundMessage, individualObserver) -> {
				if (inboundMessage.id == 2) {
					holder[0].reportErrorAfterTasksAndInboundComplete(error);
				}
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);
		holder[0] = testSubject;

		fakeOutboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfInboundMessages));
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

		fakeOutboundObserver.startMessageDelivery(testSubject, new InboundMessageProducer(1));
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
		fakeOutboundObserver.unreadyDurationMillis = 10L;
		final var userExecutor = new LoggingExecutor("userExecutor", 5);
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {

				individualObserver.setOnReadyHandler(() -> {
					synchronized (individualObserver) {
						handlerCallCounters[inboundMessage.id - 1]++;
						if (log.isLoggable(Level.FINE)) {
							log.fine("handler for message " + inboundMessage.id + " called "
									+ handlerCallCounters[inboundMessage.id - 1] + " times");
						}
						individualObserver.notify();
					}
				});

				userExecutor.execute(() -> {
					for (int i = 0; i < numberOfResultsPerInboundMessage; i++) {
						synchronized (individualObserver) {
							while (
								!individualObserver.isReady()
								|| handlerCallCounters[inboundMessage.id - 1] == 0//wait for initial
							) {
								try {
									individualObserver.wait();
								} catch (InterruptedException ignored) {}
							}
						}
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
					}
					individualObserver.onCompleted();
				});
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		fakeOutboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfInboundMessages));
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

		assertEquals("handler of the first observer should be called once",
				1, handlerCallCounters[0]);
		assertEquals("handler of the second observer should be called twice",
				2, handlerCallCounters[1]);
		assertEquals(
			"correct number of messages should be written",
			numberOfInboundMessages * numberOfResultsPerInboundMessage,
			fakeOutboundObserver.getOutputData().size()
		);
		userExecutor.verify();
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testAsyncSequentialProcessingOf40Messages() throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = 6;
		fakeOutboundObserver.unreadyDurationMillis = 3L;
		testAsyncProcessing(40, 3L, 4L, 1, 1, 2000L);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testAsyncProcessingOf100MessagesIn5Threads() throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = 13;
		fakeOutboundObserver.unreadyDurationMillis = 3L;
		testAsyncProcessing(100, 3L, 4L, 3, 5, 2000L);
	}

	void testAsyncProcessing(
		final int numberOfInboundMessages,
		final long maxInboundDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int numOfResultsPerInboundMessage,
		final int maxConcurrentInboundMessages,
		final long timeoutMillis
	) throws InterruptedException {
		final var userExecutor = new LoggingExecutor("userExecutor", maxConcurrentInboundMessages);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentInboundMessages,
			(inboundMessage, individualObserver) -> {
				final var responseCount = new AtomicInteger(0);
				for (int i = 0; i < numOfResultsPerInboundMessage; i++) {
					final var resultMessageNumber = i;
					userExecutor.execute(new Runnable() {

						@Override public void run() {
							simulateProcessingDelay(
								maxProcessingDelayMillis,
								inboundMessage.id + responseCount.get()
							);
							individualObserver.onNext(new OutboundMessage(inboundMessage.id));
							if (responseCount.incrementAndGet() == numOfResultsPerInboundMessage) {
								individualObserver.onCompleted();
							}
						}

						@Override public String toString() {
							return "task: { inboundMessageId: " + inboundMessage.id
								+ ", resultNo: " + resultMessageNumber + " }";
						}
					});
				}
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		fakeOutboundObserver.startMessageDelivery(
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
			numberOfInboundMessages * numOfResultsPerInboundMessage,
			fakeOutboundObserver.getOutputData().size()
		);
		userExecutor.verify();
		performStandardVerifications(fakeOutboundObserver);
	}



	@Test
	public void testDispatchingOnReadyHandlerIntegrationSingleThread() throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = 6;
		fakeOutboundObserver.unreadyDurationMillis = 3L;
		testDispatchingOnReadyHandlerIntegration(10, 0L, 0L, 1, 10, 1, 2, 2000L);
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		fakeOutboundObserver.outputBufferSize = 6;
		fakeOutboundObserver.unreadyDurationMillis = 3L;
		testDispatchingOnReadyHandlerIntegration(20, 3L, 4L, 3, 5, 3, 5, 2000L);
	}

	void testDispatchingOnReadyHandlerIntegration(
		final int numberOfInboundMessages,
		final long maxInboundDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int handlerTasksPerMessage,
		final int numberOfResultMessagesPerTask,
		final int maxConcurrentInboundMessages,
		final int executorThreads,
		final long timeoutMillis
	) throws InterruptedException {
		final var userExecutor = new LoggingExecutor("userExecutor", executorThreads);
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
					(i) -> resultCounters[i] < numberOfResultMessagesPerTask
				);
			},
			interruptThreadErrorHandler(Thread.currentThread())
		);

		fakeOutboundObserver.startMessageDelivery(
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
			numberOfInboundMessages * handlerTasksPerMessage * numberOfResultMessagesPerTask,
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



	/**
	 * Change the below value if you need logging:<br/>
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message received/sent and every task dispatched
	 * to {@link LoggingExecutor}.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static Level LOG_LEVEL = Level.WARNING;

	static final Logger log = Logger.getLogger(ConcurrentInboundObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		try {
			LOG_LEVEL = Level.parse(System.getProperty(
					ConcurrentInboundObserverTest.class.getPackageName() + ".level"));
		} catch (Exception ignored) {}
		log.setLevel(LOG_LEVEL);
		FakeOutboundObserver.getLogger().setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}
}
