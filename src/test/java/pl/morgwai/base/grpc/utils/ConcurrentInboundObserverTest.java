// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;
import io.grpc.stub.*;

import org.junit.*;

import pl.morgwai.base.grpc.utils.FakeOutboundObserver.LoggingExecutor;

import static org.junit.Assert.*;



public abstract class ConcurrentInboundObserverTest {



	/**
	 * Timeout for single-threaded, no-processing-delay operations.
	 */
	public static final long TIMEOUT_MILLIS = 500L;



	FakeOutboundObserver<OutboundMessage, Integer> outboundObserver;
	LoggingExecutor grpcInternalExecutor;

	@Before
	public void setup() {
		grpcInternalExecutor = new LoggingExecutor("grpcInternalExecutor", 10);
		outboundObserver = new FakeOutboundObserver<>(grpcInternalExecutor);
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

	BiConsumer<Throwable,
			ConcurrentInboundObserver<InboundMessage, OutboundMessage, OutboundMessage>>
					newErrorHandler(Thread thread) {
		return (error, thisObserver) -> thread.interrupt();
	}



	/**
	 * Simulates client/previous chained call delivering inbound messages.
	 * For use with {@link FakeOutboundObserver#startMessageDelivery(StreamObserver, Consumer)}.
	 * Instances created in test methods.
	 */
	static class InboundMessageProducer implements Consumer<StreamObserver<InboundMessage>> {

		final int numberOfMessages;
		final long maxMessageDeliveryDelayMillis;

		InboundMessageProducer(int numberOfMessages, long maxMessageDeliveryDelayMillis) {
			this.numberOfMessages = numberOfMessages;
			this.maxMessageDeliveryDelayMillis = maxMessageDeliveryDelayMillis;
		}

		InboundMessageProducer(int numberOfMessages) { this(numberOfMessages, 0L); }



		@Override
		public void accept(StreamObserver<InboundMessage> testSubject) {
			// exit if all messages have been already delivered
			if (messageIdSequence >= numberOfMessages) return;

			// deliver the next message immediately or after a slight delay
			if (maxMessageDeliveryDelayMillis > 0L) {
				try {
					Thread.sleep(messageIdSequence % (maxMessageDeliveryDelayMillis + 1L));
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



		@Override public String toString() { return ""; }
	}



	@Test
	public void testSynchronousProcessingOutboundObserverAlwaysReady() throws InterruptedException {
		final var numberOfMessages = 10;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfMessages, outboundObserver.getOutputData().size());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testSynchronousProcessingOutboundObserverUnreadySometimes()
			throws InterruptedException {
		final var numberOfMessages = 15;
		outboundObserver.outputBufferSize = 4;
		outboundObserver.unreadyDurationMillis = 3L;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				individualObserver.onNext(new OutboundMessage(inboundMessage.id));
				individualObserver.onCompleted();
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfMessages, outboundObserver.getOutputData().size());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testOnErrorSingleThread() throws InterruptedException {
		final var numberOfMessages = 2;
		final var error = new Exception();
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				if (inboundMessage.id > 1) {
					outboundObserver.onError(
							new Exception("no messages should be requested after an error"));
				}
				individualObserver.onError(error);
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertSame("supplied error should be reported", error, outboundObserver.getReportedError());
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testReportErrorAfterTasksAndInboundComplete() throws InterruptedException {
		final var numberOfMessages = 10;
		final var error = new Exception();
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
			newErrorHandler(Thread.currentThread())
		);
		holder[0] = testSubject;

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
			testSubject, new InboundMessageProducer(numberOfMessages));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
			numberOfMessages, outboundObserver.getOutputData().size());
		assertSame("supplied error should be reported", error, outboundObserver.getReportedError());
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfMessages = 1;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
						individualObserver.onCompleted();
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
						exceptionThrownHolder[0] = false;
					} catch (IllegalStateException e) {
						exceptionThrownHolder[0] = true;
					}
					exceptionThrownHolder.notify();
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		synchronized (exceptionThrownHolder) {
			while (exceptionThrownHolder[0] == null) {
				exceptionThrownHolder.wait(getRemainingMillis(startMillis));
				if (getRemainingMillis(startMillis) <= 1L) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfMessages = 1;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
						individualObserver.onCompleted();
						individualObserver.onCompleted();
						exceptionThrownHolder[0] = false;
					} catch (IllegalStateException e) {
						exceptionThrownHolder[0] = true;
					}
					exceptionThrownHolder.notify();
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		synchronized (exceptionThrownHolder) {
			while (exceptionThrownHolder[0] == null) {
				exceptionThrownHolder.wait(getRemainingMillis(startMillis));
				if (getRemainingMillis(startMillis) <= 1L) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfMessages = 1;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
						individualObserver.onCompleted();
						individualObserver.onError(new Exception());
						exceptionThrownHolder[0] = false;
					} catch (IllegalStateException e) {
						exceptionThrownHolder[0] = true;
					}
					exceptionThrownHolder.notify();
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		synchronized (exceptionThrownHolder) {
			while (exceptionThrownHolder[0] == null) {
				exceptionThrownHolder.wait(getRemainingMillis(startMillis));
				if (getRemainingMillis(startMillis) <= 1L) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testAsyncSequentialProcessingOf40Messages() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3L;
		testAsyncProcessing(40, 3L, 4L, 1, 1, 2000L);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testAsyncProcessingOf100MessagesIn5Threads() throws InterruptedException {
		outboundObserver.outputBufferSize = 13;
		outboundObserver.unreadyDurationMillis = 3L;
		testAsyncProcessing(100, 3L, 4L, 3, 5, 2000L);
	}

	void testAsyncProcessing(
		final int numberOfMessages,
		final long maxMessageDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int resultsPerMessage,
		final int maxConcurrentMessages,
		final long timeoutMillis
	) throws InterruptedException {
		final var userExecutor = new LoggingExecutor("userExecutor", maxConcurrentMessages);
		final var halfProcessingDelay = maxProcessingDelayMillis / 2L;
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentMessages,
			(inboundMessage, individualObserver) -> {
				final var responseCount = new AtomicInteger(0);
				for (int i = 0; i < resultsPerMessage; i++) {
					// produce each response in a separate task in about 1-3ms
					final var resultMessageNumber = Integer.valueOf(i);
					userExecutor.execute(new Runnable() {

						@Override public void run() {
							// sleep time varies depending on request/response message counts
							final var processingDelay = halfProcessingDelay + (
								(inboundMessage.id + responseCount.get()) % halfProcessingDelay
							);
							try {
								Thread.sleep(processingDelay);
							} catch (InterruptedException ignored) {}
							individualObserver.onNext(
									new OutboundMessage(inboundMessage.id));
							if (responseCount.incrementAndGet() == resultsPerMessage) {
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
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject,
				new InboundMessageProducer(numberOfMessages, maxMessageDeliveryDelayMillis));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis, timeoutMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis, timeoutMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis, timeoutMillis));

		assertEquals("correct number of messages should be written",
				numberOfMessages * resultsPerMessage, outboundObserver.getOutputData().size());
		verifyExecutor(userExecutor);
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testIndividualOnReadyHandlersAreCalledProperly() throws InterruptedException {
		final var userExecutor = new LoggingExecutor("userExecutor", 5);
		final int[] handlerCallCounters = {0, 0};
		final var numberOfMessages = 2;
		final int resultsPerMessage = 2;
		outboundObserver.outputBufferSize = numberOfMessages * resultsPerMessage - 1;
		outboundObserver.unreadyDurationMillis = 1L;
		final var testSubject = newConcurrentInboundObserver(
			1,
			(inboundMessage, individualObserver) -> {

				individualObserver.setOnReadyHandler(() -> {
					handlerCallCounters[inboundMessage.id - 1]++;
					synchronized (individualObserver) {
						if (log.isLoggable(Level.FINE)) {
							log.fine("handler for message " + inboundMessage.id + " called "
									+ handlerCallCounters[inboundMessage.id - 1] + " times");
						}
						individualObserver.notify();
					}
				});

				userExecutor.execute(() -> {
					final var inboundMessageId = inboundMessage.id;
					for (int i = 0; i < resultsPerMessage; i ++) {
						synchronized (individualObserver) {
							while (
								!individualObserver.isReady()
								|| handlerCallCounters[inboundMessageId - 1] < 1
								|| (inboundMessageId == 2 && handlerCallCounters[1] <= i)
							) try {
								individualObserver.wait();
							} catch (InterruptedException ignored) {}
						}
						individualObserver.onNext(new OutboundMessage(inboundMessage.id));
					}
					individualObserver.onCompleted();
				});
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("handler of the active observer should be called", 2, handlerCallCounters[1]);
		assertEquals("handler of and finalized observer should not be called",
				1, handlerCallCounters[0]);
		assertEquals("correct number of messages should be written",
				numberOfMessages * resultsPerMessage, outboundObserver.getOutputData().size());
		verifyExecutor(userExecutor);
		performStandardVerifications(outboundObserver);
	}



	@Test
	public void testDispatchingOnReadyHandlerIntegrationSingleThread() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3L;
		testDispatchingOnReadyHandlerIntegration(10, 0L, 0L, 1, 10, 1, 2, 2000L);
		assertTrue("messages should be written in order",
				Comparators.isInOrder(outboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3L;
		testDispatchingOnReadyHandlerIntegration(20, 3L, 4L, 3, 5, 3, 5, 2000L);
	}

	void testDispatchingOnReadyHandlerIntegration(
		final int numberOfMessages,
		final long maxMessageDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int tasksPerMessage,
		final int resultsPerTask,
		final int maxConcurrentMessages,
		final int executorThreads,
		final long timeoutMillis
	) throws InterruptedException {
		final var halfProcessingDelay = maxProcessingDelayMillis / 2L;
		final var userExecutor = new LoggingExecutor("userExecutor", executorThreads);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentMessages,
			(inboundMessage, individualObserver) -> {
				final int[] resultCounters = new int[tasksPerMessage];
				DispatchingOnReadyHandler.copyWithFlowControl(
					individualObserver,
					userExecutor,
					tasksPerMessage,
					(i) -> resultCounters[i] < resultsPerTask,
					(i) -> {
						if (halfProcessingDelay > 0L) {
							final var processingDelay = halfProcessingDelay +
								((inboundMessage.id + resultCounters[i]) % halfProcessingDelay);
							try {
								Thread.sleep(processingDelay);
							} catch (InterruptedException ignored) {}
						}
						resultCounters[i]++;
						return new OutboundMessage(inboundMessage.id);
					}
				);
			},
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject,
				new InboundMessageProducer(numberOfMessages, maxMessageDeliveryDelayMillis));
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis, timeoutMillis));
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis, timeoutMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis, timeoutMillis));

		assertEquals("correct number of messages should be written",
				numberOfMessages * tasksPerMessage * resultsPerTask,
				outboundObserver.getOutputData().size());
		verifyExecutor(userExecutor);
		performStandardVerifications(outboundObserver);
	}



	public static void performStandardVerifications(FakeOutboundObserver<?, ?> outboundObserver) {
		assertTrue("outbound stream should be marked as completed", outboundObserver.isFinalized());
		assertEquals("no extra finalizations should occur",
				0, outboundObserver.extraFinalizationCount.get());
		assertEquals("no messages should be written after finalization",
				0, outboundObserver.messagesAfterFinalizationCount.get());
		assertTrue("gRPC tasks shouldn't throw exceptions",
				outboundObserver.grpcInternalExecutor.getUncaughtTaskExceptions().isEmpty());
		verifyExecutor(outboundObserver.grpcInternalExecutor);
	}



	public static void verifyExecutor(LoggingExecutor executor, Throwable... expectedUncaught) {
		assertTrue("no task scheduling failures should occur on " + executor.getName(),
				executor.getRejectedTasks().isEmpty());
		assertEquals("only expected exceptions should be thrown by tasks",
				expectedUncaught.length, executor.getUncaughtTaskExceptions().size());
		for (var exception: expectedUncaught) {
			assertTrue("all expected exceptions should be thrown by tasks",
					executor.getUncaughtTaskExceptions().containsKey(exception));
		}
		if (executor.isTerminated()) return;
		final int activeCount = executor.getActiveCount();
		final var unstartedTasks = executor.shutdownNow();
		if (unstartedTasks.size() == 0 && activeCount == 0) {
			log.warning(executor.getName() + " not terminated, but no remaining tasks :?");
			return;
		}
		log.severe(executor.getName() + " has " + activeCount + " active tasks remaining");
		for (var task: unstartedTasks) log.severe(executor.getName() + " unstarted " + task);
		fail(executor.getName() + " should shutdown cleanly");
	}



	public static long getRemainingMillis(long startMillis, long timeoutMillis) {
		return Math.max(1L, timeoutMillis + startMillis - System.currentTimeMillis());
	}

	public static long getRemainingMillis(long startMillis) {
		return getRemainingMillis(startMillis, TIMEOUT_MILLIS);
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
