/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
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



public class ConcurrentRequestObserverTest {



	/**
	 * Test subject. as it is an abstract class, subclasses are created in test methods (anon ones)
	 * or using {@link ConcurrentRequestObserverTest.NoErrorConcurrentRequestObserver} helper class.
	 */
	ConcurrentRequestObserver<RequestMessage, ResponseMessage> requestObserver;

	static class NoErrorConcurrentRequestObserver
			extends ConcurrentRequestObserver<RequestMessage, ResponseMessage> {

		MessageHandler messageHandler;

		NoErrorConcurrentRequestObserver(
				StreamObserver<ResponseMessage> responseObserver, MessageHandler messageHandler) {
			super(responseObserver);
			this.messageHandler = messageHandler;
		}

		/**
		 * Lambda instances are created in test methods when calling
		 * {@link NoErrorConcurrentRequestObserver#NoErrorConcurrentRequestObserver(StreamObserver,
		 * MessageHandler)} constructor.
		 */
		public interface MessageHandler {
			void onRequest(
				RequestMessage requestMessage, StreamObserver<ResponseMessage> responseObserver);
		}

		@Override
		public void onError(Throwable t) {
			fail("unexecpted call");
		}

		@Override
		protected void onRequest(
				RequestMessage requestMessage, StreamObserver<ResponseMessage> responseObserver) {
			if (log.isLoggable(Level.FINEST)) log.finest("request received: " + requestMessage);
			messageHandler.onRequest(requestMessage, responseObserver);
		}
	}



	/**
	 * Called by {@link #responseObserver responseObserver.request(int)}. Created in test methods to
	 * simulate a given client behavior, often using {@link #getSynchronousMessageProducer(int)}.
	 */
	Runnable nextMessageRequestedHandler;

	AtomicInteger requestIdSequence;

	Runnable getSynchronousMessageProducer(int numberOfMessages) {
		return () -> {
			if (requestIdSequence.get() < numberOfMessages) {
				requestObserver.onNext(new RequestMessage(requestIdSequence.incrementAndGet()));
			} else {
				requestObserver.onCompleted();
			}
		};
	}



	FakeResponseObserver responseObserver;

	class FakeResponseObserver extends ServerCallStreamObserver<ResponseMessage> {



		/**
		 * Stores argument of {@link #onNext(ResponseMessage)}.
		 */
		List<ResponseMessage> outputData = new LinkedList<>();

		/**
		 * Response observer reports itself unready every each outputBufferSize messages are
		 * submitted to it. <code>0</code> means always ready.
		 */
		int outputBufferSize = 0;

		/**
		 * Interval after which observer will become ready again.
		 */
		long clientProcessingDelayMillis;

		volatile boolean ready = true;
		Runnable onReadyHandler;

		@Override
		public void onNext(ResponseMessage message) {
			if (log.isLoggable(Level.FINEST)) log.finest("response sent: " + message);
			outputData.add(message);

			// mark observer unready every outputBufferSize messages
			if (outputBufferSize > 0 && (outputData.size() % outputBufferSize == 0)) {
				if (log.isLoggable(Level.FINER)) log.finer("response observer unready");
				ready = false;
				executor.execute(() -> {
					// become ready again after clientProcessingDelayMillis ms
					try {
						Thread.sleep(clientProcessingDelayMillis);
					} catch (InterruptedException e) {}
					synchronized (requestObserver) {
						if ( ! ready) {
							if (log.isLoggable(Level.FINER)) log.finer("response observer ready");
							ready = true;
							onReadyHandler.run();
						}
					}
				});
			}
		}

		@Override
		public boolean isReady() {
			return ready;
		}

		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			this.onReadyHandler = onReadyHandler;
		}



		/**
		 * Counts {@link #onCompleted()} calls. Should be 1 at the end of positive test methods.
		 */
		int completedCount = 0;

		/**
		 * Stores argument of {@link #onError(Throwable)}.
		 */
		Throwable reportedError;

		Object lock = new Object();

		@Override
		public void onError(Throwable t) {
			if (log.isLoggable(Level.FINER)) log.finer("error reported: " + t);
			synchronized (lock) {
				if (reportedError != null) fail("onError called twice");
				reportedError = t;
				lock.notify();
			}
		}

		@Override
		public void onCompleted() {
			if (log.isLoggable(Level.FINER)) log.finer("response completed");
			synchronized (lock) {
				completedCount++;
				lock.notify();
			}
		}

		public void awaitFinalization(long timeoutMillis) throws InterruptedException {
			synchronized (lock) {
				if (completedCount == 0 && reportedError == null) lock.wait(timeoutMillis);
			}
		}



		boolean autoRequestDisabled = false;

		@Override
		public void request(int count) {
			if ( ! autoRequestDisabled) fail("autoRequest was not disabled");
			if (nextMessageRequestedHandler != null) {
				for (int i = 0; i < count; i++) nextMessageRequestedHandler.run();
			}
		}

		@Override
		public void disableAutoRequest() {
			this.autoRequestDisabled = true;
		}



		@Override public void disableAutoInboundFlowControl() { fail(UNEXPECTED_CALL); }
		@Override public void setCompression(String compression) { fail(UNEXPECTED_CALL); }
		@Override public void setMessageCompression(boolean enable) { fail(UNEXPECTED_CALL); }
		@Override public void setOnCancelHandler(Runnable onCancelHandler) { fail(UNEXPECTED_CALL);}
		@Override public boolean isCancelled() { fail(UNEXPECTED_CALL); return false; }
		static final String UNEXPECTED_CALL = "unexpected call";
	}



	/**
	 * Executor for async tasks, such as delivering a next message, handling a message, marking
	 * response observer as ready, etc.
	 */
	ExecutorService executor;



	@Test
	public void testSynchronousDeliveryResponseObserverAlwaysReady() throws InterruptedException {
		nextMessageRequestedHandler = getSynchronousMessageProducer(10);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
			}
		);

		responseObserver.request(1);  // runs the test, everything happens in 1 thread
		responseObserver.awaitFinalization(10_000l);

		assertEquals("all messages should be written",
				requestIdSequence.get(), responseObserver.outputData.size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.completedCount);
	}



	@Test
	public void testSynchronousDeliveryResponseObserverNotReadyOnce() throws InterruptedException {
		responseObserver.outputBufferSize = 6;
		responseObserver.clientProcessingDelayMillis = 3;
		nextMessageRequestedHandler = getSynchronousMessageProducer(10);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
			}
		);

		responseObserver.request(1);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		executor.shutdown();
		executor.awaitTermination(20, TimeUnit.MILLISECONDS);

		assertTrue("executor should shutdown cleanly", executor.isTerminated());
		assertEquals("all messages should be written",
				requestIdSequence.get(), responseObserver.outputData.size());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.completedCount);
	}



	@Test
	public void testOnError() throws InterruptedException {
		int[] deliveredCountHolder = {0};
		Exception error = new Exception();
		nextMessageRequestedHandler = getSynchronousMessageProducer(2);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				if (++deliveredCountHolder[0] > 1) {
					fail("no messages should be requested after error");
				}
				responseObserver.onError(error);;
			}
		);

		responseObserver.request(1);
		responseObserver.awaitFinalization(10_000l);

		assertSame("supplied error should be reported", error, responseObserver.reportedError);
	}



	@Test
	public void testOnNextAfterOnCompleted() {
		nextMessageRequestedHandler = getSynchronousMessageProducer(2);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
			}
		);

		try {
			responseObserver.request(1);
			fail("IllegalStateException should be thrown");
		} catch (IllegalStateException e) {}
	}



	@Test
	public void testOnCompletedTwice() {
		nextMessageRequestedHandler = getSynchronousMessageProducer(2);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
				responseObserver.onCompleted();
			}
		);

		try {
			responseObserver.request(1);
			fail("IllegalStateException should be thrown");
		} catch (IllegalStateException e) {}
	}



	@Test
	public void testOnErrorAfterOnCompleted() {
		nextMessageRequestedHandler = getSynchronousMessageProducer(2);
		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
				responseObserver.onError(new Exception());;
			}
		);

		try {
			responseObserver.request(1);
			fail("IllegalStateException should be thrown");
		} catch (IllegalStateException e) {}
	}



	@Test
	public void test100AsyncRequests() throws InterruptedException {
		responseObserver.outputBufferSize = 13;
		responseObserver.clientProcessingDelayMillis = 5;
		testAsyncRequests(100, 3, 5);
	}

	@Test
	public void test100AsyncSequentialRequests() throws InterruptedException {
		responseObserver.outputBufferSize = 7;
		responseObserver.clientProcessingDelayMillis = 3;
		testAsyncRequests(100, 1, 1);
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(responseObserver.outputData, responseComparator));
	}

	void testAsyncRequests(int requestNumber, int responsesPerRequest, int concurrencyLevel)
			throws InterruptedException {
		int[] deliveredCountHolder = {0};

		nextMessageRequestedHandler = () -> {
			int requestId = requestIdSequence.incrementAndGet();
			if (requestId > requestNumber) return;

			// deliver the next message synchronously or asynchronously after slight delay
			if (requestId % 3 == 0) {
				synchronized (requestObserver) {
					requestObserver.onNext(new RequestMessage(requestId));
					if (++deliveredCountHolder[0] == requestNumber) {
						requestObserver.onCompleted();
					}
				}
			} else {
				executor.execute(() -> {
					try {
						Thread.sleep(requestId % 2);
					} catch (InterruptedException e) {}
					synchronized (requestObserver) {
						requestObserver.onNext(new RequestMessage(requestId));
						if (++deliveredCountHolder[0] == requestNumber) {
							requestObserver.onCompleted();
						}
					}
				});
			}
		};

		requestObserver = new NoErrorConcurrentRequestObserver(
			responseObserver,
			(requestMessage, responseObserver) -> {
				final AtomicInteger responseCount = new AtomicInteger(0);
				// produce each response asynchronously in about 1-3ms
				for (int i = 0; i < responsesPerRequest; i++) {
					executor.execute(() -> {
						try {
							// sleep time may vary 1-3ms each run depending on race conditions
							Thread.sleep(((requestMessage.id + responseCount.get()) % 3) + 1);
						} catch (InterruptedException e) {}
						responseObserver.onNext(new ResponseMessage(requestMessage.id));
						if (responseCount.incrementAndGet() == responsesPerRequest) {
							responseObserver.onCompleted();
						}
					});
				}
			}
		);

		responseObserver.request(concurrencyLevel);  // runs the test
		responseObserver.awaitFinalization(10_000l);
		executor.shutdown();
		executor.awaitTermination(20, TimeUnit.MILLISECONDS);

		assertTrue("executor should shutdown cleanly", executor.isTerminated());
		assertEquals("response should be marked completed 1 time",
				1, responseObserver.completedCount);
		assertEquals("all messages should be written",
				requestNumber * responsesPerRequest, responseObserver.outputData.size());
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
		responseObserver = new FakeResponseObserver();
		requestIdSequence = new AtomicInteger(0);
		executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
	}



	// change the below value if you need logging
	// FINER will log finalizing and marking responseObserver ready/unready
	// FINEST will additionally log every message received and sent
	static final Level LOG_LEVEL = Level.OFF;

	static final Logger log =
			Logger.getLogger(ConcurrentRequestObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		var handler = new ConsoleHandler();
		handler.setLevel(LOG_LEVEL);
		log.addHandler(handler);
		log.setLevel(LOG_LEVEL);
	}
}
