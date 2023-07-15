// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.stub.ClientCallStreamObserver;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.*;
import pl.morgwai.base.grpc.utils.BlockingResponseObserver.ErrorReportedException;

import static org.junit.Assert.*;



public class BlockingResponseObserverTests extends EasyMockSupport {



	BlockingResponseObserver<Integer, ResponseMessage> responseObserver;
	List<ResponseMessage> receivedData;
	@Mock ClientCallStreamObserver<Integer> requestObserver;



	@Before
	public void setup() {
		receivedData = new LinkedList<>();
		responseObserver = new BlockingResponseObserver<>((msg) -> {
			if (log.isLoggable(Level.FINER)) log.finer("received " + msg);
			receivedData.add(msg);
		});
		requestObserver = mock(ClientCallStreamObserver.class);
	}



	@Test
	public void testPositiveCase() throws InterruptedException, ErrorReportedException {
		final ResponseMessage[] inputData = new ResponseMessage[10];
		final var worker = new Thread(() -> {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException ignored) {}
			for (int i = 0; i < inputData.length; i++) {
				inputData[i] = new ResponseMessage(i);
				responseObserver.onNext(inputData[i]);
			}
			responseObserver.onCompleted();
		});
		worker.start();
		responseObserver.awaitCompletion(50L);
		worker.join(10L);

		assertTrue("response should be marked as completed", responseObserver.isCompleted());
		assertEquals("all input messages should be received",
				inputData.length, receivedData.size());
		for (int i = 0; i < inputData.length; i++) {
			assertSame("input and received messages should be the same",
					inputData[i], receivedData.get(i));
		}
	}



	@Test
	public void testOnError() throws InterruptedException {
		final Exception reportedError = new Exception();
		final var worker = new Thread(() -> {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException ignored) {}
			responseObserver.onError(reportedError);
		});
		worker.start();
		try {
			responseObserver.awaitCompletion(50L);
			fail("ErrorReportedException should be thrown");
		} catch (ErrorReportedException e) {
			assertSame("reported error should be caught", reportedError, e.getCause());
			assertSame("reported error should be available via getError()",
					reportedError, responseObserver.getError().get());
		}
		worker.join(10L);

		assertTrue("response should be marked as completed", responseObserver.isCompleted());
	}



	@Test
	public void testTimeout() throws InterruptedException {
		final var worker = new Thread(() -> {
			try {
				Thread.sleep(20L);
			} catch (InterruptedException ignored) {}
			synchronized (responseObserver) {
				responseObserver.notifyAll();
			}
		});
		worker.start();
		final var startMillis = System.currentTimeMillis();
		final var awaitResult = responseObserver.toAwaitable().await(50L);

		assertFalse("await result should indicate failure", awaitResult);
		assertTrue("at least 50ms should pass", System.currentTimeMillis() - startMillis >= 50L);
		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
		worker.join(10L);
	}



	@Test
	public void completedBeforeAwait() throws InterruptedException, ErrorReportedException {
		responseObserver.onCompleted();
		responseObserver.awaitCompletion(1L);

		assertTrue("response should be marked as completed", responseObserver.isCompleted());
	}



	@Test
	public void testBeforeStart() {
		final var requestObserverHolder = new Object[1];
		responseObserver = new BlockingResponseObserver<>(
			(msg) -> {},
			(observer) -> requestObserverHolder[0] = observer
		);
		replayAll();
		responseObserver.beforeStart(requestObserver);

		assertSame("requestObserver should be passed to startHandler",
				requestObserver, requestObserverHolder[0]);
		assertSame("requestObserver should be available via getRequestObserver()",
				requestObserver, responseObserver.getRequestObserver().get());
		verifyAll();
	}



	static class ResponseMessage {

		final int id;

		public ResponseMessage(int id) { this.id = id; }

		@Override public String toString() { return "msg-" + id; }
	}



	// change the below value if you need logging
	// FINER will log every message received
	static Level LOG_LEVEL = Level.SEVERE;

	static final Logger log = Logger.getLogger(BlockingResponseObserverTests.class.getName());

	@BeforeClass
	public static void setupLogging() {
		try {
			LOG_LEVEL = Level.parse(System.getProperty(
					BlockingResponseObserverTests.class.getPackageName() + ".level"));
		} catch (Exception ignored) {}
		log.setLevel(LOG_LEVEL);
		for (final var handler: Logger.getLogger("").getHandlers()) handler.setLevel(LOG_LEVEL);
	}

}
