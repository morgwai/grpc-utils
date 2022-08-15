// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.samples.grpc.utils;

import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import pl.morgwai.base.concurrent.Awaitable;
import pl.morgwai.base.concurrent.Awaitable.AwaitInterruptedException;
import pl.morgwai.base.grpc.utils.BlockingResponseObserver;
import pl.morgwai.base.grpc.utils.BlockingResponseObserver.ErrorReportedException;
import pl.morgwai.base.grpc.utils.GrpcAwaitable;



/**
 * Sends sequential requests to {@link SqueezedServer#parent(StreamObserver)} method with respect to
 * flow-control.
 */
public class Client {



	public static void main(String[] args) throws Exception {
		// command line arguments
		var squeezedTarget = args.length > 0 ? args[0] : null;
		final var numberOfMessages = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
		final var messageSizeKB = args.length > 2 ? Integer.parseInt(args[2]) : 4;
		final var pauseRate = args.length > 3 ? Integer.parseInt(args[3]) : 200;
		final var pauseMillis = args.length > 4 ? Long.parseLong(args[4]) : 0L;

		// start servers if needed
		BackendServer backendServer = null;
		SqueezedServer squeezedServer = null;
		if (squeezedTarget == null
			|| squeezedTarget.equals("")
			|| squeezedTarget.equals("0")
			|| squeezedTarget.equals("null")
			|| squeezedTarget.equals("none")
			|| squeezedTarget.equals("new")
		) {
			backendServer = new BackendServer(
					0,
					30L, 3,
					10L, 8, 0);
			squeezedServer = new SqueezedServer(
					0, "localhost:" + backendServer.getPort(),
					30L, 3,
					30L, 6, 8, 4,
					15L, 12);
			squeezedTarget = "localhost:" + squeezedServer.getPort();
		}
		final Awaitable backendServerShutdown = backendServer == null
				? (timeout) -> true
				: backendServer::shutdownAndEnforceTermination;
		final Awaitable squeezedServerShutdown = squeezedServer == null
				? (timeout) -> true
				: squeezedServer::shutdownAndEnforceTermination;

		// build channel and setup shutdown hook
		final var managedChannel = ManagedChannelBuilder
			.forTarget(squeezedTarget)
			.usePlaintext()
			.build();
		final var connector = FrontendGrpc.newStub(managedChannel);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				Awaitable.awaitMultiple(
					10_000L,
					(timeout) -> {
						final var result =
								GrpcAwaitable.ofEnforcedTermination(managedChannel).await(timeout);
						if ( !result) {
							System.out.println("CLIENT: squeezed channel hasn't shutdown cleanly");
						}
						return result;
					},
					squeezedServerShutdown,
					backendServerShutdown
				);
			} catch (AwaitInterruptedException e) {
				System.out.println("await interrupted");
			} finally {
				System.out.println("bye!");
			}
		}));

		// create response observer
		final var responseCounter = new AtomicInteger(0);
		final var requestCounter = new AtomicInteger(0);
		final var message = "################################".repeat(32 * messageSizeKB);
		final var responseObserver =
				new BlockingResponseObserver<ParentRequest, ParentResponse>() {

			@Override public void onNext(ParentResponse response) {
				if (responseCounter.incrementAndGet() % pauseRate == 0) {
					System.out.println("CLIENT: got " + responseCounter.get() + " responses");
					try {
						Thread.sleep(pauseMillis);
					} catch (InterruptedException ignored) {}
					getRequestObserver().get().request(pauseRate);
				}
			}

			@Override
			public void beforeStart(ClientCallStreamObserver<ParentRequest> requestObserver) {
				super.beforeStart(requestObserver);
				requestObserver.disableAutoRequestWithInitial(pauseRate);
				requestObserver.setOnReadyHandler(() -> {
					while ((requestCounter.get() < numberOfMessages || numberOfMessages == 0)
							&& requestObserver.isReady()) {
						int id = requestCounter.incrementAndGet();
						requestObserver.onNext(ParentRequest.newBuilder()
							.setMessageId(id)
							.setMessage(message)
							.build());
						if (id % pauseRate == 0) {
							System.out.println("CLIENT: sent " + id + " requests");
						}
					}
					if (requestCounter.get() == numberOfMessages) {
						System.out.println(
								"CLIENT: sent " + requestCounter.get() + " requests total");
						requestObserver.onCompleted();
					}
				});
			}
		};

		// issue the call and await response stream completion or interrupt
		System.out.println("CLIENT: sending requests...\n");
		final var requestObserver = (ClientCallStreamObserver<?>)
				connector.parent(responseObserver);
		final var cancelHook = new Thread(
			() -> {
				System.out.println(
						"CLIENT: cancelling, sent " + requestCounter.get() + " requests total");
				requestObserver.cancel("cancelling on interrupt", null);
			}
		);
		Runtime.getRuntime().addShutdownHook(cancelHook);
		try {
			responseObserver.awaitCompletion();
			System.out.println(
					"CLIENT: squeezed response stream completed, got " + responseCounter.get());
		} catch (ErrorReportedException e) {
			System.out.println("CLIENT: error from squeezed, got " + responseCounter.get()
					+ " responses, error: " + e.getCause());
		} finally {
			try {
				Runtime.getRuntime().removeShutdownHook(cancelHook);
			} catch (Exception ignored) {}  // shutdown in progress
			System.exit(0);
		}
	}
}
