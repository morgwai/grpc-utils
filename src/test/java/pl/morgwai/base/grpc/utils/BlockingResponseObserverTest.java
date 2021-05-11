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

import static org.junit.Assert.*;



public class BlockingResponseObserverTest {



	BlockingResponseObserver<ResponseMessage> responseObserver;
	List<ResponseMessage> receivedData;
	Throwable reportedError;
	Thread responseConsumer;



	@Before
	public void setup() {
		receivedData = new LinkedList<>();
		reportedError = null;
		responseObserver = new BlockingResponseObserver<>((msg) -> {
			if (log.isLoggable(Level.FINEST)) log.finest("received " + msg);
			receivedData.add(msg);
		});
		responseConsumer = new Thread(() -> {
			try {
				responseObserver.awaitCompletion();
			} catch (Exception e) {
				reportedError = e;
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

		assertTrue("response should be marked as completed", responseObserver.isCompleted());
		assertNull("no exception should occur", reportedError);
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

		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
		assertTrue("InterruptedException should be caught",
				reportedError instanceof InterruptedException);
	}



	@Test
	public void testOnError() throws InterruptedException {
		responseConsumer = new Thread(() -> {
			try {
				responseObserver.awaitCompletion();
			} catch (Exception e) {
				reportedError = e.getCause();
			}
		});
		responseConsumer.start();
		Exception thrown = new Exception();
		responseObserver.onError(thrown);
		responseConsumer.join(10_000l);

		assertTrue("response should be marked as completed", responseObserver.isCompleted());
		assertSame("passed exception should be caught", thrown, reportedError);
	}



	@Test
	public void testTimeout() throws InterruptedException {
		responseConsumer = new Thread(() -> {
			try {
				responseObserver.awaitCompletion(1l);
			} catch (Exception e) {
				reportedError = e;
			}
		});
		responseConsumer.start();
		responseConsumer.join(10_000l);

		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
		assertNull("no exception should occur", reportedError);
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
