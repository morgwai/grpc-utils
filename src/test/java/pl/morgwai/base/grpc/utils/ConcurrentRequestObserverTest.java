/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertEquals;
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
	 * Test subject. as it is an abstract class, anonymous subclasses are created in test methods.
	 */
	ConcurrentRequestObserver<RequestMessage, ResponseMessage> requestObserver;

	 /**
	  * Called by {@link #responseObserver responseObserver.request(1)}. Created in test methods to
	  * simulate a given client behavior.
	  */
	volatile Runnable onNextMessageRequestedHandler;

	/**
	  * Called by {@link #responseObserver responseObserver.onCompleted()}. Created in test methods
	  * to notify that a given test has concluded and results should be verified.
	 */
	volatile Runnable onCompletedHandler;

	/**
	 * Increased by {@link #responseObserver responseObserver.onCompleted()}. Should be 1 at the end
	 * of positive test methods.
	 */
	AtomicInteger completedCount;

	/**
	 * {@link #responseObserver} reports itself unready every each outputBufferSize messages are
	 * submitted to it. <code>0</code> means always ready.
	 */
	int outputBufferSize = 0;

	/**
	 * Interval after which {@link #responseObserver} will become ready again.
	 */
	long clientProcessingDelayMillis;

	/**
	 * Whenever test subject calls {@link #responseObserver responseObserver.onNext(msg)} the
	 * argument will be added to this list.
	 */
	List<ResponseMessage> outputData;

	/**
	 * When test subject calls {@link #responseObserver responseObserver.onError(t)} the argument
	 * will be stored on this var.
	 */
	volatile Throwable reportedError;

	/**
	 * Initially <code>false</code>, Switched to <code>true</code> when test subject calls
	 * {@link #responseObserver responseObserver.disableAutoRequest()}. Should be verified by test
	 * methods.
	 */
	volatile boolean autoRequestDisabled;

	AtomicInteger requestIdSequence;

	ExecutorService executor;



	@Test
	public void testSingleThreadOneResponsePerRequestResponseObserverAlwaysReady() {
		requestObserver = new ConcurrentRequestObserver<>(responseObserver) {

			@Override
			public void onError(Throwable t) {
				fail("unexecpted call");
			}

			@Override
			protected void onRequest(
				RequestMessage requestMessage, StreamObserver<ResponseMessage> responseObserver
			) {
				responseObserver.onNext(new ResponseMessage(requestMessage.id));
				responseObserver.onCompleted();
			}
		};

		onNextMessageRequestedHandler = () -> {
			// deliver the next message synchronously
			if (requestIdSequence.get() < 10) {
				requestObserver.onNext(new RequestMessage(requestIdSequence.incrementAndGet()));
			} else {
				requestObserver.onCompleted();
			}
		};

		responseObserver.request(1);  // runs the test

		assertEquals("all messages should be written", requestIdSequence.get(), outputData.size());
		assertTrue("auto-request should be disabled", autoRequestDisabled);
		assertEquals("response should be marked completed 1 time", 1, completedCount.get());
	}



	@Test
	public void test100AsyncRequests() throws InterruptedException {
		outputBufferSize = 13;
		clientProcessingDelayMillis = 5;
		testAsyncRequests(100, 3, 5);
	}

	@Test
	public void test100AsyncSequentialRequests() throws InterruptedException {
		outputBufferSize = 7;
		clientProcessingDelayMillis = 3;
		testAsyncRequests(100, 1, 1);
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(outputData, responseComparator));
	}

	void testAsyncRequests(int requestNumber, int responsesPerRequest, int concurrencyLevel)
			throws InterruptedException {

		requestObserver = new ConcurrentRequestObserver<>(responseObserver) {

			@Override
			public void onError(Throwable t) {
				fail("unexecpted call");
			}

			@Override
			protected void onRequest(
				RequestMessage requestMessage,
				StreamObserver<ResponseMessage> responseObserver
			) {
				log.finest("request received: " + requestMessage);
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
		};

		int[] deliveredCountHolder = {0};

		onNextMessageRequestedHandler = () -> {
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

		onCompletedHandler = () -> {
			synchronized (this) {
				notify();
			}
		};
		responseObserver.request(concurrencyLevel);  // runs the test
		synchronized (this) {  // wait up to 10s for the onCompleted to be called
			if (completedCount.get() == 0) wait(10_000l);
		}
		executor.shutdown();
		executor.awaitTermination(20, TimeUnit.MILLISECONDS);

		assertTrue("executor should shutdown cleanly", executor.isTerminated());
		assertEquals("response should be marked completed 1 time", 1, completedCount.get());
		assertEquals("all messages should be written",
				requestNumber * responsesPerRequest, outputData.size());
		assertTrue("auto-request should be disabled", autoRequestDisabled);
	}



	ServerCallStreamObserver<ResponseMessage> responseObserver = new ServerCallStreamObserver<>() {

		volatile boolean responseObserverReady = true;
		volatile Runnable onReadyHandler;

		@Override
		public void request(int count) {
			if (onNextMessageRequestedHandler != null) {
				for (int i = 0; i < count; i++) onNextMessageRequestedHandler.run();
			}
		}

		@Override
		public void onNext(ResponseMessage message) {
			log.finest("reply sent: " + message);
			outputData.add(message);
			if (outputBufferSize > 0 && (outputData.size() % outputBufferSize == 0)) {
				log.finer("response observer unready");
				responseObserverReady = false;
				executor.execute(() -> {
					try {
						Thread.sleep(clientProcessingDelayMillis);
					} catch (InterruptedException e) {}
					synchronized (requestObserver) {
						if ( ! responseObserverReady) {
							log.finer("response observer ready");
							responseObserverReady = true;
							onReadyHandler.run();
						}
					}
				});
			}
		}

		@Override
		public void onError(Throwable t) {
			log.finer("error reported: " + t);
			reportedError = t;
		}

		@Override
		public void onCompleted() {
			log.finer("response completed");
			completedCount.incrementAndGet();
			if (onCompletedHandler != null) onCompletedHandler.run();
		}

		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			this.onReadyHandler = onReadyHandler;
		}

		@Override
		public boolean isReady() {
			return responseObserverReady;
		}

		@Override
		public void disableAutoRequest() {
			autoRequestDisabled = true;
		}

		@Override
		public void disableAutoInboundFlowControl() {
			fail("should never be called");
		}

		@Override
		public boolean isCancelled() {
			fail("should never be called");
			return false;
		}

		@Override
		public void setOnCancelHandler(Runnable onCancelHandler) {
			fail("should never be called");
		}

		@Override
		public void setCompression(String compression) {
			fail("should never be called");
		}

		@Override
		public void setMessageCompression(boolean enable) {
			fail("should never be called");
		}
	};



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
		completedCount = new AtomicInteger(0);
		outputData = new LinkedList<>();
		reportedError = null;
		autoRequestDisabled = false;
		requestIdSequence = new AtomicInteger(0);
		executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
	}



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
