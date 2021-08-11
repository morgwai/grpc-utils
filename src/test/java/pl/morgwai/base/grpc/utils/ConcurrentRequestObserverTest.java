// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;



public class ConcurrentRequestObserverTest {



	/**
	 * Test subject. As it is an abstract class, subclasses are created in test methods (anon ones)
	 * or using {@link NoErrorConcurrentRequestObserver} helper class.
	 */
	ConcurrentRequestObserver<RequestMessage, ResponseMessage> requestObserver;



	FakeResponseObserver<ResponseMessage> responseObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	ExecutorService grpcInternalExecutor;

	/**
	 * This lock ensures that user's request observer will be called by at most 1 thread
	 * concurrently, just as gRPC listener does. It is exposed for cases when user code simulates
	 * gRPC listener behavior, usually when triggering a test.
	 */
	Object listenerLock;



	void deliverNextRequest() {
		int requestId = requestIdSequence.incrementAndGet();
		if (requestId > numberOfRequests) return;

		// deliver the next message asynchronously immediately or after a slight delay
		if (maxRequestDeliveryDelayMillis > 0l) {
			try {
				Thread.sleep(requestId % (maxRequestDeliveryDelayMillis + 1));
			} catch (InterruptedException e) {}
		}

		synchronized (listenerLock) {
			requestObserver.onNext(new RequestMessage(requestId));
		}

		boolean allDelivered;
		synchronized (this) {
			allDelivered = (++deliveredRequestCount == numberOfRequests);
		}
		if (allDelivered) {
			synchronized (listenerLock) {
				requestObserver.onCompleted();
			}
		}
	}

	int numberOfRequests;
	AtomicInteger requestIdSequence;
	int deliveredRequestCount;
	long maxRequestDeliveryDelayMillis;



	@Before
	public void setup() {
		requestIdSequence = new AtomicInteger(0);
		deliveredRequestCount = 0;
		maxRequestDeliveryDelayMillis = 0l;
		grpcInternalExecutor = new ThreadPoolExecutor(
				10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
		responseObserver.messageProducer =() -> deliverNextRequest();
		listenerLock = responseObserver.getListenerLock();
		requestObserver = new ConcurrentRequestObserver<>(
				responseObserver,
				null,
				(error) -> fail("unexpected call"));
	}



	@Test
	public void testSynchronousProcessingResponseObserverAlwaysReady() throws InterruptedException {
		numberOfRequests = 10;
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			singleRequestMessageResponseObserver.onNext(new ResponseMessage(requestMessage.id));
			singleRequestMessageResponseObserver.onCompleted();
		};

		synchronized (listenerLock) {
			responseObserver.request(1);  // runs the test, everything happens in 1 thread
		}
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertEquals("all messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	@Test
	public void testSynchronousProcessingResponseObserverUnreadySometimes()
			throws InterruptedException {
		numberOfRequests = 15;
		responseObserver.outputBufferSize = 4;
		responseObserver.unreadyDurationMillis = 3;
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			singleRequestMessageResponseObserver.onNext(new ResponseMessage(requestMessage.id));
			singleRequestMessageResponseObserver.onCompleted();
		};

		synchronized (listenerLock) {
			responseObserver.request(1);  // runs the test
		}
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertEquals("all messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	@Test
	public void testOnError() throws InterruptedException {
		numberOfRequests = 2;
		Exception error = new Exception();
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			if (deliveredRequestCount > 1) {
				fail("no messages should be requested after an error");
			}
			singleRequestMessageResponseObserver.onError(error);
		};

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertSame("supplied error should be reported", error, responseObserver.getReportedError());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					exceptionThrownHolder[0] = false;
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onCompleted();
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onError(new Exception());;
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of the executor",
				responseObserver.getExecuteAfterShutdown());
	}



	void testAsyncProcessing(
			long maxProcessingDelayMillis, int responsesPerRequest, int concurrencyLevel)
			throws InterruptedException {
		ExecutorService userExecutor = new ThreadPoolExecutor(
				concurrencyLevel, concurrencyLevel, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		boolean[] executeAfterShutdownHolder = {false};
		long halfProcessingDelay = maxProcessingDelayMillis / 2;

		requestObserver.requestHandler = (requestMessage, singleRequestMessageResponseObserver) -> {
			final AtomicInteger responseCount = new AtomicInteger(0);
			// produce each response asynchronously in about 1-3ms
			for (int i = 0; i < responsesPerRequest; i++) {
				try {
					userExecutor.execute(() -> {
						// sleep time varies depending on request/response message counts
						var processingDelay = halfProcessingDelay +
								((requestMessage.id + responseCount.get()) % halfProcessingDelay);
						try {
							Thread.sleep(processingDelay);
						} catch (InterruptedException e) {}
						singleRequestMessageResponseObserver.onNext(
								new ResponseMessage(requestMessage.id));
						if (responseCount.incrementAndGet() == responsesPerRequest) {
							singleRequestMessageResponseObserver.onCompleted();
						}
					});
				} catch (Exception e) {
					synchronized (executeAfterShutdownHolder) {
						executeAfterShutdownHolder[0] = true;
					}
				}
			}
		};

		synchronized (listenerLock) {
			responseObserver.request(concurrencyLevel);  // runs the test
		}
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100 +
				Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("all messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no tasks should be executed after the shutdown of grpcExecutor",
				responseObserver.getExecuteAfterShutdown());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		synchronized (executeAfterShutdownHolder) {
			assertFalse("no tasks should be executed after the shutdown of userExecutor",
					executeAfterShutdownHolder[0]);
		}
	}

	@Test
	public void testAsyncProcessing100Requests5Threads() throws InterruptedException {
		numberOfRequests = 100;
		maxRequestDeliveryDelayMillis = 3;
		responseObserver.outputBufferSize = 13;
		responseObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(4l, 3, 5);
	}

	@Test
	public void testAsyncSequentialProcessing40Requests() throws InterruptedException {
		numberOfRequests = 40;
		maxRequestDeliveryDelayMillis = 3;
		responseObserver.outputBufferSize = 6;
		responseObserver.unreadyDurationMillis = 3;
		testAsyncProcessing(4l, 1, 1);
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.getOutputData(), responseComparator));
	}



	static class RequestMessage {

		int id;

		public RequestMessage(int id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "request-" + id;
		}
	}

	static class ResponseMessage {

		int requestId;

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
	 * <code>FINE</code> will log finalizing events.<br/>
	 * <code>FINER</code> will log marking observer ready/unready.<br/>
	 * <code>FINEST</code> will log every message received and sent and synchronization events
	 */
	static final Level LOG_LEVEL = Level.OFF;

	static final Logger log =
			Logger.getLogger(ConcurrentRequestObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		var handler = new ConsoleHandler();
		handler.setLevel(LOG_LEVEL);
		log.addHandler(handler);
		log.setLevel(LOG_LEVEL);
		var responseObserverLog = FakeResponseObserver.getLogger();
		responseObserverLog.addHandler(handler);
		responseObserverLog.setLevel(LOG_LEVEL);
	}
}
