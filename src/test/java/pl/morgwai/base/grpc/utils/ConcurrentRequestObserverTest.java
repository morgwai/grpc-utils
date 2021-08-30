// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import pl.morgwai.base.grpc.utils.FakeResponseObserver.FailureTrackingThreadPoolExecutor;

import static org.junit.Assert.*;



public class ConcurrentRequestObserverTest {



	FakeResponseObserver<ResponseMessage> responseObserver;
	FailureTrackingThreadPoolExecutor grpcInternalExecutor;

	@Before
	public void setup() {
		grpcInternalExecutor = new FailureTrackingThreadPoolExecutor(10);
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
	}



	/**
	 * Simulates client delivering request messages.
	 * For use with {@link FakeResponseObserver#startRequestDelivery(StreamObserver, Consumer)}.
	 * Instances created in test methods.
	 */
	static class RequestProducer implements Consumer<StreamObserver<RequestMessage>> {

		final int numberOfRequests;
		final long maxRequestDeliveryDelayMillis;

		RequestProducer(int numberOfRequests, long maxRequestDeliveryDelayMillis) {
			this.numberOfRequests = numberOfRequests;
			this.maxRequestDeliveryDelayMillis = maxRequestDeliveryDelayMillis;
		}

		RequestProducer(int numberOfRequests) { this(numberOfRequests, 0l); }



		@Override
		public void accept(StreamObserver<RequestMessage> requestObserver) {
			// exit if all requests have been already delivered
			if (requestIdSequence >= numberOfRequests) return;

			// deliver the next message immediately or after a slight delay
			if (maxRequestDeliveryDelayMillis > 0l) {
				try {
					Thread.sleep(requestIdSequence % (maxRequestDeliveryDelayMillis + 1));
				} catch (InterruptedException e) {}
			}

			synchronized (deliveryLock) {
				if (requestIdSequence >= numberOfRequests) return;
				final var requestMessage = new RequestMessage(++requestIdSequence);
				if (log.isLoggable(Level.FINER)) log.finer("delivering " + requestMessage);
				requestObserver.onNext(requestMessage);
				if (requestIdSequence == numberOfRequests) {
					log.fine("half-closing");
					requestObserver.onCompleted();
				}
			}
		}

		volatile int requestIdSequence = 0;
		final Object deliveryLock = new Object(); // ensures requests are delivered in order



		@Override public String toString() { return ""; }
	}



	/**
	 * Delegate test subject creation to this protected method so that tests of subclasses of
	 * ConcurrentRequestObserver can reuse this test class.
	 * @see OrderedConcurrentRequestObserverTest
	 */
	protected ConcurrentRequestObserver<RequestMessage, ResponseMessage>
			newConcurrentRequestObserver(
					int numberOfConcurrentRequests,
					BiConsumer<RequestMessage, CallStreamObserver<ResponseMessage>> requestHandler,
					Consumer<Throwable> errorHandler
	) {
		return new ConcurrentRequestObserver<>(
				responseObserver,
				numberOfConcurrentRequests,
				requestHandler,
				errorHandler);
	}

	Consumer<Throwable> newErrorHandler(Thread thread) {
		return (error) -> thread.interrupt();
	}



	@Test
	public void testSynchronousProcessingResponseObserverAlwaysReady() throws InterruptedException {
		final var numberOfRequests = 10;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				individualObserver.onNext(new ResponseMessage(requestMessage.id));
				individualObserver.onCompleted();
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.getOutputData(), responseComparator));
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testSynchronousProcessingResponseObserverUnreadySometimes()
			throws InterruptedException {
		final var numberOfRequests = 15;
		responseObserver.outputBufferSize = 4;
		responseObserver.unreadyDurationMillis = 3l;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				individualObserver.onNext(new ResponseMessage(requestMessage.id));
				individualObserver.onCompleted();
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.getOutputData(), responseComparator));
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testOnError() throws InterruptedException {
		final var numberOfRequests = 2;
		final var error = new Exception();
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				if (requestMessage.id > 1) {
					responseObserver.onError(
							new Exception("no messages should be requested after an error"));
				}
				individualObserver.onError(error);
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertSame("supplied error should be reported", error, responseObserver.getReportedError());
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfRequests = 1;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new ResponseMessage(requestMessage.id));
						individualObserver.onCompleted();
						individualObserver.onNext(new ResponseMessage(requestMessage.id));
						exceptionThrownHolder[0] = false;
					} catch (IllegalStateException e) {
						exceptionThrownHolder[0] = true;
					}
					exceptionThrownHolder.notify();
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfRequests = 1;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new ResponseMessage(requestMessage.id));
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

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		final Boolean[] exceptionThrownHolder = { null };
		final var numberOfRequests = 1;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {
				synchronized (exceptionThrownHolder) {
					try {
						individualObserver.onNext(new ResponseMessage(requestMessage.id));
						individualObserver.onCompleted();
						individualObserver.onError(new Exception());;
						exceptionThrownHolder[0] = false;
					} catch (IllegalStateException e) {
						exceptionThrownHolder[0] = true;
					}
					exceptionThrownHolder.notify();
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
	}



	@Test
	public void testAsyncSequentialProcessingOf40Requests() throws InterruptedException {
		responseObserver.outputBufferSize = 6;
		responseObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(40, 3l, 4l, 1, 1);
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.getOutputData(), responseComparator));
	}

	@Test
	public void testAsyncProcessingOf100RequestsIn5Threads() throws InterruptedException {
		responseObserver.outputBufferSize = 13;
		responseObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(100, 3l, 4l, 3, 5);
	}

	void testAsyncProcessing(
		final int numberOfRequests,
		final long maxRequestDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int responsesPerRequest,
		final int numberOfConcurrentRequests
	) throws InterruptedException {
		final var userExecutor = new FailureTrackingThreadPoolExecutor(numberOfConcurrentRequests);
		final var halfProcessingDelay = maxProcessingDelayMillis / 2;
		final var requestObserver = newConcurrentRequestObserver(
			numberOfConcurrentRequests,
			(requestMessage, individualObserver) -> {
				final var responseCount = new AtomicInteger(0);
				// produce each response asynchronously in about 1-3ms
				for (int i = 0; i < responsesPerRequest; i++) {
					final var responseNumber = Integer.valueOf(i);
					final var task = new Runnable() {

						@Override
						public void run() {
							// sleep time varies depending on request/response message counts
							final var processingDelay = halfProcessingDelay + (
								(requestMessage.id + responseCount.get()) % halfProcessingDelay
							);
							try {
								Thread.sleep(processingDelay);
							} catch (InterruptedException e) {}
							individualObserver.onNext(
									new ResponseMessage(requestMessage.id));
							if (responseCount.incrementAndGet() == responsesPerRequest) {
								individualObserver.onCompleted();
							}
						}

						@Override
						public String toString() {
							return "task: { requestId: " + requestMessage.id + ", responseNo: "
									+ responseNumber + " }";
						}
					};
					if (log.isLoggable(Level.FINER)) log.finer("scheduling " + task);
					userExecutor.execute(task);
				}
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver,
				new RequestProducer(numberOfRequests, maxRequestDeliveryDelayMillis));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10l, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
		verifyExecutor(userExecutor, "userExecutor");
	}



	@Test
	public void testIndividualOnReadyHandlersAreCalledProperly() throws InterruptedException {
		final var userExecutor = new FailureTrackingThreadPoolExecutor(5);
		final int[] handlerCallCounters = {0, 0};
		final var numberOfRequests = 2;
		final int responsesPerRequest = 2;
		responseObserver.outputBufferSize = numberOfRequests * responsesPerRequest - 1;
		responseObserver.unreadyDurationMillis = 1l;
		final var requestObserver = newConcurrentRequestObserver(
			1,
			(requestMessage, individualObserver) -> {

				individualObserver.setOnReadyHandler(() -> {
					handlerCallCounters[requestMessage.id - 1]++;
					if (log.isLoggable(Level.FINE)) {
						log.fine("handler for request " + requestMessage.id + " called "
								+ handlerCallCounters[requestMessage.id - 1] + " times");
					}
					synchronized (individualObserver) {
						individualObserver.notify();
					}
				});

				userExecutor.execute(() -> {
					for (int i = 0; i < responsesPerRequest; i ++) {
						synchronized (individualObserver) {
							while ( ! individualObserver.isReady()
									|| handlerCallCounters[requestMessage.id - 1] < 1) try {
								individualObserver.wait();
							} catch (InterruptedException e) {}
						}
						individualObserver.onNext(new ResponseMessage(requestMessage.id));
					}
					individualObserver.onCompleted();
				});
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver, new RequestProducer(numberOfRequests));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10l, TimeUnit.MILLISECONDS);

		assertEquals("handler of the active observer should be called", 2, handlerCallCounters[1]);
		assertEquals("handler of and finalized observer should not be called",
				1, handlerCallCounters[0]);
		assertEquals("correct number of messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
		verifyExecutor(userExecutor, "userExecutor");
	}



	@Test
	public void testDispatchingOnReadyHandlerIntegrationSingleThread() throws InterruptedException {
		responseObserver.outputBufferSize = 6;
		responseObserver.unreadyDurationMillis = 3l;
		testDispatchingOnReadyHandlerIntegration(10, 0l, 0l, 1, 10, 1, 2);
		assertTrue("messages should be written in order",
				Comparators.isInOrder(responseObserver.getOutputData(), responseComparator));
	}

	@Test
	public void testDispatchingOnReadyHandlerIntegrationMultiThread() throws InterruptedException {
		responseObserver.outputBufferSize = 6;
		responseObserver.unreadyDurationMillis = 3l;
		testDispatchingOnReadyHandlerIntegration(20, 3l, 4l, 3, 5, 3, 5);
	}

	void testDispatchingOnReadyHandlerIntegration(
		final int numberOfRequests,
		final long maxRequestDeliveryDelayMillis,
		final long maxProcessingDelayMillis,
		final int tasksPerRequest,
		final int responsesPerTask,
		final int numberOfConcurrentRequests,
		final int executorThreads
	) throws InterruptedException {
		final var halfProcessingDelay = maxProcessingDelayMillis / 2;
		final var userExecutor = new FailureTrackingThreadPoolExecutor(executorThreads);
		final var requestObserver = newConcurrentRequestObserver(
			numberOfConcurrentRequests,
			(requestMessage, individualObserver) -> {
				final int[] responseCounters = new int[tasksPerRequest];
				individualObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
					individualObserver,
					userExecutor,
					tasksPerRequest,
					(i) -> responseCounters[i] >= responsesPerTask,
					(i) -> {
						if (halfProcessingDelay > 0) {
							final var processingDelay = halfProcessingDelay +
								((requestMessage.id + responseCounters[i]) % halfProcessingDelay);
							try {
								Thread.sleep(processingDelay);
							} catch (InterruptedException e) {}
						}
						responseCounters[i]++;
						return new ResponseMessage(requestMessage.id);
					}
				));
			},
			newErrorHandler(Thread.currentThread())
		);

		responseObserver.startRequestDelivery(
				requestObserver,
				new RequestProducer(numberOfRequests, maxRequestDeliveryDelayMillis));
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(100l, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10l, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests * tasksPerRequest * responsesPerTask,
				responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		verifyExecutor(grpcInternalExecutor, "grpcInternalExecutor");
		verifyExecutor(userExecutor, "userExecutor");
	}



	public static void verifyExecutor(FailureTrackingThreadPoolExecutor executor, String name) {
		assertTrue("no task scheduling failures should occur on " + name,
				executor.getSubmissionFailures().isEmpty());
		if (executor.isTerminated()) return;
		final var stuckTasks = executor.shutdownNow();
		if (stuckTasks.size() == 0) return;
		for (var stuckTask: stuckTasks) log.severe("stuck " + stuckTask);
		fail(name + " should shutdown cleanly");
	}



	static class RequestMessage {

		final int id;

		public RequestMessage(int id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "request-" + id;
		}
	}

	static class ResponseMessage {

		final int requestId;

		public ResponseMessage(int requestId) {
			this.requestId = requestId;
		}

		@Override
		public String toString() {
			return "response-" + requestId;
		}
	}

	static Comparator<ResponseMessage> responseComparator =
			(msg1, msg2) -> Integer.compare(msg1.requestId, msg2.requestId);



	/**
	 * Change the below value if you need logging:<br/>
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message received/sent.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static Level LOG_LEVEL = Level.SEVERE;

	static final Logger log = Logger.getLogger(ConcurrentRequestObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		try {
			LOG_LEVEL = Level.parse(System.getProperty(
					ConcurrentRequestObserverTest.class.getPackageName() + ".level"));
		} catch (Exception e) {}
		log.setLevel(LOG_LEVEL);
		FakeResponseObserver.getLogger().setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}
}
