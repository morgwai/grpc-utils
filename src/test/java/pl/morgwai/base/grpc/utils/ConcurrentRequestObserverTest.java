// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;

import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import pl.morgwai.base.grpc.utils.FakeResponseObserver.FailureTrackingThreadPoolExecutor;

import static org.junit.Assert.*;



public class ConcurrentRequestObserverTest {



	/**
	 * Test subject. As it is an abstract class, subclasses are created in test methods (anon ones)
	 * or using {@link NoErrorConcurrentRequestObserver} helper class.
	 */
	ConcurrentRequestObserver<RequestMessage, ResponseMessage> requestObserver;



	/**
	 * Simulates client delivering request messages. Controlled by adjusting values of the below
	 * {@link #numberOfRequests} and {@link #maxRequestDeliveryDelayMillis} variables in test
	 * methods.
	 */
	void deliverNextRequest(StreamObserver<RequestMessage> requestObserver) {
		// exit if all requests have been already delivered
		int currentId;
		synchronized (deliveryLock) {
			currentId = requestIdSequence;
			if (requestIdSequence >= numberOfRequests) return;
		}

		// deliver the next message immediately or after a slight delay
		if (maxRequestDeliveryDelayMillis > 0l) {
			try {
				Thread.sleep(currentId % (maxRequestDeliveryDelayMillis + 1));
			} catch (InterruptedException e) {}
		}

		synchronized (deliveryLock) {
			if (requestIdSequence >= numberOfRequests) return;
			var requestMessage = new RequestMessage(++requestIdSequence);
			if (log.isLoggable(Level.FINER)) log.finer("delivering " + requestMessage);
			requestObserver.onNext(requestMessage);
			if (requestIdSequence == numberOfRequests) {
				log.fine("half-closing");
				requestObserver.onCompleted();
			}
		}
	}

	int numberOfRequests;  // must be set in test methods
	long maxRequestDeliveryDelayMillis;  // may be overridden in test methods
	Object deliveryLock = new Object();
	int requestIdSequence;

	FakeResponseObserver<ResponseMessage> responseObserver;

	/**
	 * Executor for gRPC internal tasks, such as delivering a next message, marking response
	 * observer as ready, etc.
	 */
	FailureTrackingThreadPoolExecutor grpcInternalExecutor;






	@Before
	public void setup() {
		requestIdSequence = 0;
		maxRequestDeliveryDelayMillis = 0l;
		grpcInternalExecutor = new FailureTrackingThreadPoolExecutor(10);
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
		requestObserver = newConcurrentRequestObserver();
		responseObserver.setBiDi(requestObserver, (observer)-> deliverNextRequest(observer));
	}

	protected ConcurrentRequestObserver<RequestMessage, ResponseMessage>
			newConcurrentRequestObserver() {
		return new ConcurrentRequestObserver<>(
				responseObserver,
				null,
				(error) -> fail("unexpected call"));
	}


	@Test
	public void testSynchronousProcessingResponseObserverAlwaysReady() throws InterruptedException {
		numberOfRequests = 10;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			individualObserver.onNext(new ResponseMessage(requestMessage.id));
			individualObserver.onCompleted();
		};

		responseObserver.request(1);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	@Test
	public void testSynchronousProcessingResponseObserverUnreadySometimes()
			throws InterruptedException {
		numberOfRequests = 15;
		responseObserver.outputBufferSize = 4;
		responseObserver.unreadyDurationMillis = 3l;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			individualObserver.onNext(new ResponseMessage(requestMessage.id));
			individualObserver.onCompleted();
		};

		responseObserver.request(1);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	@Test
	public void testOnError() throws InterruptedException {
		numberOfRequests = 2;
		Exception error = new Exception();
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			if (requestMessage.id > 1) {
				fail("no messages should be requested after an error");
			}
			individualObserver.onError(error);
		};

		responseObserver.request(1);
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertSame("supplied error should be reported", error, responseObserver.getReportedError());
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					individualObserver.onNext(
							new ResponseMessage(requestMessage.id));
					individualObserver.onCompleted();
					individualObserver.onNext(
							new ResponseMessage(requestMessage.id));
					exceptionThrownHolder[0] = false;
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		responseObserver.request(1);
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					individualObserver.onNext(
							new ResponseMessage(requestMessage.id));
					individualObserver.onCompleted();
					individualObserver.onCompleted();
					exceptionThrownHolder[0] = false;
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		responseObserver.request(1);
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		Boolean[] exceptionThrownHolder = { null };
		numberOfRequests = 1;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			synchronized (exceptionThrownHolder) {
				try {
					individualObserver.onNext(
							new ResponseMessage(requestMessage.id));
					individualObserver.onCompleted();
					individualObserver.onError(new Exception());;
					exceptionThrownHolder[0] = false;
				} catch (IllegalStateException e) {
					exceptionThrownHolder[0] = true;
				}
				exceptionThrownHolder.notify();
			}
		};

		responseObserver.request(1);
		synchronized (exceptionThrownHolder) {
			if (exceptionThrownHolder[0] == null) exceptionThrownHolder.wait();
		}
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);

		assertTrue("IllegalStateException should be thrown", exceptionThrownHolder[0]);
		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
	}



	void testAsyncProcessing(
			long maxProcessingDelayMillis, int responsesPerRequest, int concurrencyLevel)
			throws InterruptedException {
		var userExecutor = new FailureTrackingThreadPoolExecutor(concurrencyLevel);
		long halfProcessingDelay = maxProcessingDelayMillis / 2;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			final var responseCount = new AtomicInteger(0);
			// produce each response asynchronously in about 1-3ms
			for (int i = 0; i < responsesPerRequest; i++) {
				userExecutor.execute(() -> {
					// sleep time varies depending on request/response message counts
					var processingDelay = halfProcessingDelay +
							((requestMessage.id + responseCount.get()) % halfProcessingDelay);
					try {
						Thread.sleep(processingDelay);
					} catch (InterruptedException e) {}
					individualObserver.onNext(
							new ResponseMessage(requestMessage.id));
					if (responseCount.incrementAndGet() == responsesPerRequest) {
						individualObserver.onCompleted();
					}
				});
			}
		};

		responseObserver.request(concurrencyLevel);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
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



	@Test
	public void testIndividualOnReadyHandlerAreCalledProperly() throws InterruptedException {
		var userExecutor = new FailureTrackingThreadPoolExecutor(5);
		int[] handlerCallCounters = {0, 0};
		numberOfRequests = 2;
		int responsesPerRequest = 2;
		responseObserver.outputBufferSize = numberOfRequests * responsesPerRequest - 1;
		responseObserver.unreadyDurationMillis = 1l;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			individualObserver.setOnReadyHandler(() -> {
				handlerCallCounters[requestMessage.id - 1]++;
				synchronized (individualObserver) {
					individualObserver.notify();
				}
			});
			userExecutor.execute(() -> {
				for (int i = 0; i < responsesPerRequest; i ++) {
					synchronized (individualObserver) {
						while ( ! individualObserver.isReady()) {
							try {
								individualObserver.wait();
							} catch (InterruptedException e) {}
						}
					}
					individualObserver.onNext(new ResponseMessage(requestMessage.id));
				}
				individualObserver.onCompleted();
			});
		};

		responseObserver.request(1);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("handler of and active observer should be called",
				2, handlerCallCounters[1]);
		assertEquals("handler of and finalized observer should not be called",
				1, handlerCallCounters[0]);
		assertEquals("correct number of messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
	}



	@Test
	public void testDispatchingOnReadyHandlerIntegration() throws InterruptedException {
		var userExecutor = new FailureTrackingThreadPoolExecutor(5);
		numberOfRequests = 10;
		int responsesPerRequest = 10;
		responseObserver.outputBufferSize = 6;
		responseObserver.unreadyDurationMillis = 3l;
		requestObserver.requestHandler = (requestMessage, individualObserver) -> {
			int[] responseCountHolder = {0};
			individualObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
				individualObserver,
				userExecutor,
				() -> responseCountHolder[0] >= responsesPerRequest,
				() -> {
					responseCountHolder[0]++;
					return new ResponseMessage(requestMessage.id);
				}
			));
		};

		responseObserver.request(1);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		var executorShutdownTimeoutMillis = 100l
				+ Math.max(responseObserver.unreadyDurationMillis, maxRequestDeliveryDelayMillis);
		grpcInternalExecutor.shutdown();
		userExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(executorShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
		userExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);

		assertEquals("correct number of messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertTrue("grpcExecutor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on grpcInternalExecutor",
				grpcInternalExecutor.hadFailures());
		assertTrue("userExecutor should shutdown cleanly", userExecutor.isTerminated());
		assertFalse("no task scheduling failures should occur on userExecutor",
				userExecutor.hadFailures());
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
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message received/sent.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
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
