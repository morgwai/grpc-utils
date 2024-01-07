// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.logging.*;

import io.grpc.stub.ClientCallStreamObserver;
import org.easymock.EasyMock;
import org.junit.*;
import pl.morgwai.base.grpc.utils.BlockingResponseObserver.ErrorReportedException;

import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

import static org.junit.Assert.*;
import static pl.morgwai.base.jul.JulConfigurator.*;



public class BlockingResponseObserverTests {



	BlockingResponseObserver<Integer, ResponseMessage> responseObserver;
	List<ResponseMessage> receivedData;



	@Before
	public void setup() {
		receivedData = new LinkedList<>();
		responseObserver = new BlockingResponseObserver<>(
			(msg) -> {
				if (log.isLoggable(Level.FINER)) log.finer("received " + msg);
				receivedData.add(msg);
			}
		);
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
		final var timeoutMillis = 30L;
		final var startMillis = System.currentTimeMillis();

		assertFalse("await result should indicate failure",
				responseObserver.toAwaitable().await(timeoutMillis));
		assertTrue("at least " + timeoutMillis + "ms should pass",
				System.currentTimeMillis() - startMillis >= timeoutMillis);
		assertFalse("response should not be marked as completed", responseObserver.isCompleted());
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
		final ClientCallStreamObserver<Integer> requestObserver =
				EasyMock.createMock(ClientCallStreamObserver.class);
		EasyMock.replay(requestObserver);

		responseObserver.beforeStart(requestObserver);
		assertSame("requestObserver should be passed to startHandler",
				requestObserver, requestObserverHolder[0]);
		assertSame("requestObserver should be available via getRequestObserver()",
				requestObserver, responseObserver.getRequestObserver().get());
		EasyMock.verify(requestObserver);
	}



	static class ResponseMessage {

		final int id;

		public ResponseMessage(int id) { this.id = id; }

		@Override public String toString() { return "msg-" + id; }
	}



	static final Logger log = Logger.getLogger(BlockingResponseObserverTests.class.getName());



	/** {@code FINER} will log every message received. */
	@BeforeClass
	public static void setupLogging() {
		addOrReplaceLoggingConfigProperties(Map.of(
			LEVEL_SUFFIX, WARNING.toString(),
			ConsoleHandler.class.getName() + LEVEL_SUFFIX, FINEST.toString()
		));
		overrideLogLevelsWithSystemProperties("pl.morgwai");
	}
}
