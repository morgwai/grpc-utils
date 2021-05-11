/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.morgwai.base.grpc.utils.BlockingResponseObserver.ErrorReportedException;

import static org.junit.Assert.*;



public class BlockingResponseObserverTest {



	BlockingResponseObserver<ResponseMessage> responseObserver;
	List<ResponseMessage> receivedData;
	Throwable caughtThrowable;
	Thread responseConsumer;



	@Before
	public void setup() {
		receivedData = new LinkedList<>();
		caughtThrowable = null;
		responseObserver = new BlockingResponseObserver<>((msg) -> {
			if (log.isLoggable(Level.FINEST)) log.finest("received " + msg);
			receivedData.add(msg);
		});
		responseConsumer = new Thread(() -> {
			try {
				responseObserver.awaitCompletion();
			} catch (ErrorReportedException e) {
				caughtThrowable = e.getCause();
			} catch (Exception e) {
				caughtThrowable = e;
			}
		});
	}



	@Test
	public void testPositiveCase() throws InterruptedException {
		responseConsumer.start();
		ResponseMessage[] inputData = new ResponseMessage[10];
		for (int i = 0; i < inputData.length; i++) {
			inputData[i] = new ResponseMessage(i);
			responseObserver.onNext(inputData[i]);
		}
		responseObserver.onCompleted();
		responseConsumer.join(10_000l);

		assertFalse("awaitCompletion() should exit", responseConsumer.isAlive());
		assertTrue("response should be marked as completed", responseObserver.isCompleted());
		assertNull("no exception should occur", caughtThrowable);
		assertEquals("all input messages should be received",
				inputData.length, receivedData.size());
		for (int i = 0; i < inputData.length; i++) {
			assertSame("input and received messages should be the same",
					inputData[i], receivedData.get(i));
		}
	}



	@Test
	public void testInterrupt() throws InterruptedException {
		responseConsumer.start();
		responseConsumer.interrupt();
		responseConsumer.join(10_000l);

		assertFalse("awaitCompletion() should exit", responseConsumer.isAlive());
		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
		assertTrue("InterruptedException should be caught",
				caughtThrowable instanceof InterruptedException);
	}



	@Test
	public void testOnError() throws InterruptedException {
		responseConsumer.start();
		Exception reportedError = new Exception();
		responseObserver.onError(reportedError);
		responseConsumer.join(10_000l);

		assertFalse("awaitCompletion() should exit", responseConsumer.isAlive());
		assertTrue("response should be marked as completed", responseObserver.isCompleted());
		assertSame("reported error should be caught", reportedError, caughtThrowable);
	}



	@Test
	public void testTimeout() throws InterruptedException {
		responseConsumer = new Thread(() -> {
			try {
				responseObserver.awaitCompletion(1l);
			} catch (ErrorReportedException e) {
				caughtThrowable = e.getCause();
			} catch (Exception e) {
				caughtThrowable = e;
			}
		});
		responseConsumer.start();
		responseConsumer.join(10_000l);

		assertFalse("awaitCompletion() should exit", responseConsumer.isAlive());
		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
		assertNull("no exception should occur", caughtThrowable);
	}



	static class ResponseMessage {

		int id;

		public ResponseMessage(int id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return "msg-" + id;
		}
	}



	// change the below value if you need logging
	// FINEST will log every message received
	static final Level LOG_LEVEL = Level.OFF;

	static final Logger log =
			Logger.getLogger(BlockingResponseObserverTest.class.getName());

	@BeforeClass
	public static void setupLogging() {
		var handler = new ConsoleHandler();
		handler.setLevel(LOG_LEVEL);
		log.addHandler(handler);
		log.setLevel(LOG_LEVEL);
	}
}
