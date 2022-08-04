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
	public static final long TIMEOUT_MILLIS = 500l;



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
	protected abstract ConcurrentInboundObserver<InboundMessage, OutboundMessage>
			newConcurrentInboundObserver(
		int maxConcurrentMessages,
		BiConsumer<InboundMessage, CallStreamObserver<OutboundMessage>> messageHandler,
		Consumer<Throwable> errorHandler
	);

	Consumer<Throwable> newErrorHandler(Thread thread) {
		return (error) -> thread.interrupt();
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

		InboundMessageProducer(int numberOfMessages) { this(numberOfMessages, 0l); }



		@Override
		public void accept(StreamObserver<InboundMessage> testSubject) {
			// exit if all messages have been already delivered
			if (messageIdSequence >= numberOfMessages) return;

			// deliver the next message immediately or after a slight delay
			if (maxMessageDeliveryDelayMillis > 0l) {
				try {
					Thread.sleep(messageIdSequence % (maxMessageDeliveryDelayMillis + 1));
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
		assertEquals("outbound stream should be marked completed 1 time",
				1, outboundObserver.getFinalizedCount());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
		verifyExecutor(grpcInternalExecutor);
	}



	@Test
	public void testSynchronousProcessingOutboundObserverUnreadySometimes()
			throws InterruptedException {
		final var numberOfMessages = 15;
		outboundObserver.outputBufferSize = 4;
		outboundObserver.unreadyDurationMillis = 3l;
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
		assertEquals("outbound stream should be marked completed 1 time",
				1, outboundObserver.getFinalizedCount());
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
		verifyExecutor(grpcInternalExecutor);
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
		assertEquals("onError() should be called 1 time", 1, outboundObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor);
	}



	@Test
	public void testOnErrorMultipleThreads() throws InterruptedException {
		final var maxConcurrentMessages = 4;
		final var numberOfSeriesToPass = 10;
		final var numberOfMessages = (numberOfSeriesToPass + 2) * maxConcurrentMessages;
		final var error = new Exception();
		final var userExecutor = new LoggingExecutor("userExecutor", maxConcurrentMessages);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentMessages,
			(inboundMessage, individualObserver) -> userExecutor.execute(() -> {
				final var requestId = inboundMessage.id;
				if (requestId <= numberOfSeriesToPass * maxConcurrentMessages) {
					try {
						Thread.sleep(3l);  // processing delay
					} catch (InterruptedException ignored) {}
					individualObserver.onNext(new OutboundMessage(requestId));
					individualObserver.onCompleted();
				} else if (requestId > (numberOfSeriesToPass + 1) * maxConcurrentMessages) {
					outboundObserver.onError(
							new Exception("no messages should be requested after an error"));
				} else {
					try {
						individualObserver.onError(error);
					} catch (IllegalStateException ignored) {}  // subsequent calls will throw
				}
			}),
			newErrorHandler(Thread.currentThread())
		);

		final var startMillis = System.currentTimeMillis();
		outboundObserver.startMessageDelivery(
				testSubject, new InboundMessageProducer(numberOfMessages));
		while (outboundObserver.getFinalizedCount() < maxConcurrentMessages) {
			outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		}
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));
		userExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertEquals("correct number of messages should be written",
				numberOfSeriesToPass * maxConcurrentMessages,
				outboundObserver.getOutputData().size());
		assertSame("supplied error should be reported", error, outboundObserver.getReportedError());
		assertEquals("onError() should be called maxConcurrentMessages times",
				maxConcurrentMessages, outboundObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
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
				if (getRemainingMillis(startMillis) <= 1l) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor);
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
				if (getRemainingMillis(startMillis) <= 1l) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor);
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
				if (getRemainingMillis(startMillis) <= 1l) break;
			}
		}
		outboundObserver.awaitFinalization(getRemainingMillis(startMillis));
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(getRemainingMillis(startMillis));

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor);
	}



	@Test
	public void testAsyncSequentialProcessingOf40Messages() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(40, 3l, 4l, 1, 1, 2000l);
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				outboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testAsyncProcessingOf100MessagesIn5Threads() throws InterruptedException {
		outboundObserver.outputBufferSize = 13;
		outboundObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(100, 3l, 4l, 3, 5, 2000l);
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
		final var halfProcessingDelay = maxProcessingDelayMillis / 2;
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
		assertEquals("outbound stream should be marked completed 1 time",
				1, outboundObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testIndividualOnReadyHandlersAreCalledProperly() throws InterruptedException {
		final var userExecutor = new LoggingExecutor("userExecutor", 5);
		final int[] handlerCallCounters = {0, 0};
		final var numberOfMessages = 2;
		final int resultsPerMessage = 2;
		outboundObserver.outputBufferSize = numberOfMessages * resultsPerMessage - 1;
		outboundObserver.unreadyDurationMillis = 1l;
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
								! individualObserver.isReady()
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
		assertEquals("outbound stream should be marked completed 1 time",
				1, outboundObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	@Test
	public void testDispatchingOnReadyHandlerIntegrationSingleThread() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3l;
		testDispatchingOnReadyHandlerIntegration(10, 0l, 0l, 1, 10, 1, 2, 2000l);
		assertTrue("messages should be written in order",
				Comparators.isInOrder(outboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Test
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		outboundObserver.outputBufferSize = 6;
		outboundObserver.unreadyDurationMillis = 3l;
		testDispatchingOnReadyHandlerIntegration(20, 3l, 4l, 3, 5, 3, 5, 2000l);
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
		final var halfProcessingDelay = maxProcessingDelayMillis / 2;
		final var userExecutor = new LoggingExecutor("userExecutor", executorThreads);
		final var testSubject = newConcurrentInboundObserver(
			maxConcurrentMessages,
			(inboundMessage, individualObserver) -> {
				final int[] resultCounters = new int[tasksPerMessage];
				final var onReadyHandler = new DispatchingOnReadyHandler<>(
					individualObserver,
					userExecutor,
					tasksPerMessage,
					(i) -> resultCounters[i] >= resultsPerTask,
					(i) -> {
						if (halfProcessingDelay > 0) {
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
				onReadyHandler.setTaskToStringHandler((i) ->
					"onReadyHandler: { inboundMessageId: " + inboundMessage.id
					+ ", task: " + i
					+ ", resultNo: " + resultCounters[i] + " }"
				);
				individualObserver.setOnReadyHandler(onReadyHandler);
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
		assertEquals("outbound stream should be marked completed 1 time",
				1, outboundObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor);
		verifyExecutor(userExecutor);
	}



	public static void verifyExecutor(LoggingExecutor executor) {
		assertTrue("no task scheduling failures should occur on " + executor.getName(),
				executor.getRejectedTasks().isEmpty());
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
		return Math.max(1l, timeoutMillis + startMillis - System.currentTimeMillis());
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

	static Comparator<OutboundMessage> outboundMessageComparator =
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
