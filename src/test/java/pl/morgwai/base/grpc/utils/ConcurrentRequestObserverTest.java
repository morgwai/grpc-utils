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

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

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

	/**
	 * Convenience concrete subclass, that takes functional {@link MessageHandler} as a constructor
	 * param.
	 */
	static class NoErrorConcurrentRequestObserver
			extends ConcurrentRequestObserver<RequestMessage, ResponseMessage> {

		/**
		 * Lambda instances are created in test methods when calling
		 * {@link NoErrorConcurrentRequestObserver#NoErrorConcurrentRequestObserver(StreamObserver,
		 * MessageHandler)} constructor.
		 */
		public interface MessageHandler {
			void onRequest(
				RequestMessage requestMessage,
				StreamObserver<ResponseMessage> singleRequestMessageResponseObserver);
		}

		MessageHandler messageHandler;

		NoErrorConcurrentRequestObserver(
				ServerCallStreamObserver<ResponseMessage> responseObserver,
				MessageHandler messageHandler) {
			super(responseObserver);
			this.messageHandler = messageHandler;
		}

		@Override
		protected void onRequest(
				RequestMessage requestMessage,
				StreamObserver<ResponseMessage> singleRequestMessageResponseObserver) {
			if (log.isLoggable(Level.FINEST)) log.finest("request received: " + requestMessage);
			messageHandler.onRequest(requestMessage, singleRequestMessageResponseObserver);
		}

		@Override
		public void onError(Throwable t) {
			fail("unexecpted call");
		}
	}



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




	AtomicInteger requestIdSequence;

	Runnable newMessageProducer(int numberOfRequests, long maxDelayMillis) {
		int[] deliveredCountHolder = {0};
		return () -> {
			int requestId = requestIdSequence.incrementAndGet();
			if (requestId > numberOfRequests) return;

			// deliver the next message asynchronously immediately or after a slight delay
			if (maxDelayMillis > 0) {
				try {
					Thread.sleep(requestId % (maxDelayMillis + 1));
				} catch (InterruptedException e) {}
			}

			synchronized (listenerLock) {
				requestObserver.onNext(new RequestMessage(requestId));
			}

			boolean allDelivered;
			synchronized (deliveredCountHolder) {
				allDelivered = (++deliveredCountHolder[0] == numberOfRequests);
			}
			if (allDelivered) {
				synchronized (listenerLock) {
					requestObserver.onCompleted();
				}
			}
		};
	}

	Runnable newMessageProducer(int numberOfRequests) {
		return newMessageProducer(numberOfRequests, 0);
	}



	@Test
	public void testSynchronousProcessingResponseObserverAlwaysReady() throws InterruptedException {
		int numberOfRequests = 10;
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(numberOfRequests));
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				singleRequestMessageResponseObserver.onNext(new ResponseMessage(requestMessage.id));
				singleRequestMessageResponseObserver.onCompleted();
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);  // runs the test, everything happens in 1 thread
		}
		responseObserver.awaitFinalization(10_000l);

		assertEquals("all messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
	}



	@Test
	public void testSynchronousProcessingResponseObserverUnreadyOnce() throws InterruptedException {
		int numberOfRequests = 10;
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(numberOfRequests));
		responseObserver.setOutputBufferSize(6);
		responseObserver.setUnreadyDuration(3);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				singleRequestMessageResponseObserver.onNext(new ResponseMessage(requestMessage.id));
				singleRequestMessageResponseObserver.onCompleted();
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);  // runs the test
		}
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(20, TimeUnit.MILLISECONDS);

		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertEquals("all messages should be written",
				numberOfRequests, responseObserver.getOutputData().size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
	}



	@Test
	public void testOnError() throws InterruptedException {
		int[] deliveredCountHolder = {0};
		Exception error = new Exception();
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(2));
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				if (++deliveredCountHolder[0] > 1) {
					fail("no messages should be requested after error");
				}
				singleRequestMessageResponseObserver.onError(error);
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		responseObserver.awaitFinalization(10_000l);

		assertSame("supplied error should be reported", error, responseObserver.getReportedError());
	}



	@Test
	public void testOnNextAfterOnCompleted() throws InterruptedException {
		boolean[] resultHolder = { false };
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(2));
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
				} catch (IllegalStateException e) {
					resultHolder[0] = true;
				}
				synchronized (resultHolder) {
					resultHolder.notify();
				}
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (resultHolder) {
			resultHolder.wait();
		}
		if ( ! resultHolder[0]) fail("IllegalStateException should be thrown");
	}



	@Test
	public void testOnCompletedTwice() throws InterruptedException {
		boolean[] resultHolder = { false };
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(2));
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onCompleted();
				} catch (IllegalStateException e) {
					resultHolder[0] = true;
				}
				synchronized (resultHolder) {
					resultHolder.notify();
				}
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (resultHolder) {
			resultHolder.wait();
		}
		if ( ! resultHolder[0]) fail("IllegalStateException should be thrown");
	}



	@Test
	public void testOnErrorAfterOnCompleted() throws InterruptedException {
		boolean[] resultHolder = { false };
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(2));
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				try {
					singleRequestMessageResponseObserver.onNext(
							new ResponseMessage(requestMessage.id));
					singleRequestMessageResponseObserver.onCompleted();
					singleRequestMessageResponseObserver.onError(new Exception());;
				} catch (IllegalStateException e) {
					resultHolder[0] = true;
				}
				synchronized (resultHolder) {
					resultHolder.notify();
				}
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(1);
		}
		synchronized (resultHolder) {
			resultHolder.wait();
		}
		if ( ! resultHolder[0]) fail("IllegalStateException should be thrown");
	}



	@Test
	public void testAsyncProcessing100Requests5Threads() throws InterruptedException {
		responseObserver.setOutputBufferSize(13);
		responseObserver.setUnreadyDuration(5);
		testAsyncProcessing(100, 3, 5);
	}

	@Test
	public void testAsyncSequentialProcessing100Requests() throws InterruptedException {
		responseObserver.setOutputBufferSize(7);
		responseObserver.setUnreadyDuration(3);
		testAsyncProcessing(100, 1, 1);
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.getOutputData(), responseComparator));
	}

	void testAsyncProcessing(int numberOfRequests, int responsesPerRequest, int concurrencyLevel)
			throws InterruptedException {
		responseObserver.setNextMessageRequestedHandler(newMessageProducer(numberOfRequests, 5));
		ExecutorService userExecutor = new ThreadPoolExecutor(
				concurrencyLevel, concurrencyLevel, 0, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, singleRequestMessageResponseObserver) -> {
				final AtomicInteger responseCount = new AtomicInteger(0);
				// produce each response asynchronously in about 1-3ms
				for (int i = 0; i < responsesPerRequest; i++) {
					userExecutor.execute(() -> {
						try {
							// sleep time varies 1-3ms depending on request/response message counts
							Thread.sleep(((requestMessage.id + responseCount.get()) % 3) + 1);
						} catch (InterruptedException e) {}
						singleRequestMessageResponseObserver.onNext(
								new ResponseMessage(requestMessage.id));
						if (responseCount.incrementAndGet() == responsesPerRequest) {
							singleRequestMessageResponseObserver.onCompleted();
						}
					});
				}
			}
		);

		synchronized (listenerLock) {
			responseObserver.request(concurrencyLevel);  // runs the test
		}
		responseObserver.awaitFinalization(10_000l);
		grpcInternalExecutor.shutdown();
		grpcInternalExecutor.awaitTermination(20, TimeUnit.MILLISECONDS);

		assertTrue("executor should shutdown cleanly", grpcInternalExecutor.isTerminated());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.getFinalizedCount());
		assertEquals("all messages should be written",
				numberOfRequests * responsesPerRequest, responseObserver.getOutputData().size());
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



	@Before
	public void setup() {
		requestIdSequence = new AtomicInteger(0);
		grpcInternalExecutor = new ThreadPoolExecutor(
				10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		responseObserver = new FakeResponseObserver<>(grpcInternalExecutor);
		listenerLock = responseObserver.getListenerLock();
	}



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
