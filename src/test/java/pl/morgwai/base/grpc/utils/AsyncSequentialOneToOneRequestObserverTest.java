/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Comparators;

import io.grpc.stub.ServerCallStreamObserver;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;



public class AsyncSequentialOneToOneRequestObserverTest {



	/**
	 * Test subject. as it is an abstract class, anonymous subclasses are created in test methods
	 */
	AsyncSequentialOneToOneRequestObserver<RequestMessage, ResponseMessage> requestObserver;

	 /**
	  * Called by {@link #responseObserver responseObserver.request(1)}. Created in test methods to
	  * simulate a given client behavior.
	  */
	volatile Runnable onNextMessageRequestedHandler;

	/**
	  * Called by {@link #responseObserver responseObserver.onCompleted()}. Created in test methods,
	  * usually notifies that a given test has concluded and results should be verified.
	 */
	volatile Runnable onCompletedHandler;

	/**
	 * Will be returned when the test subject calls {@link #responseObserver
	 * responseObserver.isReady()}. Initially <code>true</code>.
	 */
	volatile boolean responseObserverReady;

	/**
	 * Must be called by a test method whenever it switches {@link #responseObserverReady} from
	 * <code>false</code> to <code>true</code> to notify the test subject.
	 */
	volatile Runnable onReadyHandler;

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
	 * Initially <code>false</code>, switched to true when test subject calls
	 * {@link #responseObserver responseObserver.disableAutoRequest()}.
	 */
	volatile boolean autoRequestDisabled;

	volatile int requestIdSequence;

	ExecutorService executor;



	@Test
	public void testSingleThreadAndResponseObserverAlwaysReady() {
		int[] completedCountHolder = {0};
		onCompletedHandler = () -> completedCountHolder[0]++;

		requestObserver = new AsyncSequentialOneToOneRequestObserver<>(responseObserver) {

			@Override
			public void onError(Throwable t) {}

			@Override
			protected void onNext(RequestMessage requestMessage,
					OneResponseObserver<ResponseMessage> responseObserver) {
				// reply instantly
				responseObserver.onResponse(new ResponseMessage(requestMessage.id));
			}
		};

		onNextMessageRequestedHandler = () -> {
			// deliver the next message synchronously
			if (requestIdSequence < 10) {
				requestObserver.onNext(new RequestMessage(++requestIdSequence));
			} else {
				requestObserver.onCompleted();
			}
		};
		onNextMessageRequestedHandler.run();

		assertEquals("all messages should be written", requestIdSequence, outputData.size());
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(outputData, responseComparator));
		assertTrue("auto-request should be disabled", autoRequestDisabled);
		assertEquals("response should be marked completed 1 time", 1, completedCountHolder[0]);
	}



	@Test
	public void testAsync500Messages() throws InterruptedException {
		int[] completedCountHolder = {0};

		onCompletedHandler = () -> {
			synchronized (completedCountHolder) {
				completedCountHolder[0]++;
				completedCountHolder.notify();
			}
		};

		requestObserver = new AsyncSequentialOneToOneRequestObserver<>(responseObserver) {

			@Override
			public void onError(Throwable t) {}

			@Override
			protected void onNext(
				RequestMessage requestMessage,
				OneResponseObserver<ResponseMessage> responseObserver
			) {
				// process request message asynchronously in about 1-3ms
				executor.execute(() -> {
					try {
						Thread.sleep((requestMessage.id % 3) + 1);
					} catch (InterruptedException e) {}
					responseObserver.onResponse(new ResponseMessage(requestMessage.id));
				});
			}
		};

		onNextMessageRequestedHandler = () -> {
			if (requestIdSequence < 500) {
				int requestId = ++requestIdSequence;

				// make buffer full every 7 messages and make it ready again after about 3-5ms
				if (requestId % 7 == 0) {
					responseObserverReady = false;
					executor.execute(() -> {
						try {
							Thread.sleep((requestId % 3) + 3);
						} catch (InterruptedException e) {}
						responseObserverReady = true;
						synchronized (requestObserver) {
							onReadyHandler.run();
						}
					});
				}

				// deliver the next message synchronously or asynchronously after slight delay
				if (requestId % 3 == 0) {
					synchronized (requestObserver) {
						requestObserver.onNext(new RequestMessage(requestId));
					}
				} else {
					executor.execute(() -> {
						try {
							Thread.sleep(requestId % 2);
						} catch (InterruptedException e) {}
						synchronized (requestObserver) {
							requestObserver.onNext(new RequestMessage(requestId));
						}
					});
				}

			} else {
				synchronized (requestObserver) {
					requestObserver.onCompleted();
				}
			}
		};
		onNextMessageRequestedHandler.run();

		synchronized (completedCountHolder) {
			// wait up to 10s for the onCompleted to be called
			if (completedCountHolder[0] == 0) completedCountHolder.wait(10_000l);
		}

		assertEquals("response should be marked completed 1 time", 1, completedCountHolder[0]);
		assertEquals("all messages should be written", requestIdSequence, outputData.size());
		assertTrue("messages should be written in order",
				Comparators.isInStrictOrder(outputData, responseComparator));
		assertTrue("auto-request should be disabled", autoRequestDisabled);
		assertEquals("executor should shutdown cleanly", 0, executor.shutdownNow().size());
	}



	ServerCallStreamObserver<ResponseMessage> responseObserver = new ServerCallStreamObserver<>() {

		@Override
		public void request(int count) {
			if (count != 1) throw new IllegalArgumentException();
			if (onNextMessageRequestedHandler != null) {
				onNextMessageRequestedHandler.run();
			}
		}

		@Override
		public void onNext(ResponseMessage message) {
			log.finest("message sent: " + message);
			outputData.add(message);
		}

		@Override
		public void onError(Throwable t) {
			log.finer("error reported: " + t);
			reportedError = t;
		}

		@Override
		public void onCompleted() {
			log.finer("response completed");
			onCompletedHandler.run();
		}

		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			AsyncSequentialOneToOneRequestObserverTest.this.onReadyHandler = onReadyHandler;
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



	public static class RequestMessage {

		int id;

		public RequestMessage(int id) {
			this.id = id;
		}
	}

	public static class ResponseMessage {

		int requestId;

		public ResponseMessage(int requestId) {
			this.requestId = requestId;
		}

		@Override
		public String toString() {
			return "msg-" + requestId;
		}
	}

	static Comparator<ResponseMessage> responseComparator =
			(msg1, msg2) -> Integer.compare(msg1.requestId, msg2.requestId);



	@Before
	public void setup() {
		outputData = new LinkedList<>();
		reportedError = null;
		onReadyHandler = null;
		autoRequestDisabled = false;
		requestIdSequence = 0;
		responseObserverReady = true;
		executor = new ThreadPoolExecutor(3, 3, 0l, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
	}



	static final Level LOG_LEVEL = Level.OFF;

	static final Logger log =
			Logger.getLogger(AsyncSequentialOneToOneRequestObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		var handler = new ConsoleHandler();
		handler.setLevel(LOG_LEVEL);
		log.addHandler(handler);
		log.setLevel(LOG_LEVEL);
	}
}
