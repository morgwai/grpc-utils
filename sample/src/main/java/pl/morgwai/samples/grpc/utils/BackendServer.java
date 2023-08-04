// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.samples.grpc.utils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.stub.*;
import pl.morgwai.base.utils.concurrent.Awaitable;
import pl.morgwai.base.grpc.utils.ConcurrentInboundObserver;
import pl.morgwai.base.grpc.utils.GrpcAwaitable;
import pl.morgwai.samples.grpc.utils.BackendGrpc.BackendImplBase;

import static pl.morgwai.samples.grpc.utils.SqueezedServer.produce;



/**
 * Implements {@code Backend} service. Both {@link #nested(StreamObserver)} and
 * {@link #chained(StreamObserver)} methods process their request streams concurrently in multiple
 * threads and send responses with respect to flow-control.
 */
public class BackendServer extends BackendImplBase {



	final Server grpcServer;
	final ExecutorService executor;

	final long nestedCallProcessingMillis;
	final int maxNestedCallConcurrentRequests;

	final long chainedCallProcessingMillis;
	final int maxChainedCallConcurrentRequests;
	final int sendErrorAt;



	/**
	 * Domain logic interface.
	 * Instances are created at the beginning of each invocation of
	 * {@link #nested(StreamObserver)} and {@link #chained(StreamObserver)} methods.
	 */
	interface BackendCallProcessor {
		void abort();
		NestedResponse process(NestedRequest request) throws InterruptedException;
		ChainedResponse process(ChainedRequest request) throws IOException, InterruptedException;
	}



	@Override
	public StreamObserver<NestedRequest> nested(
			StreamObserver<NestedResponse> basicResponseObserver) {
		System.out.println("BACKEND: got nested call");
		final var responseObserver =
			(ServerCallStreamObserver<NestedResponse>) basicResponseObserver;
		final BackendCallProcessor processor = new BackendCallProcessorMock();
		responseObserver.setOnCancelHandler(
			() -> {
				processor.abort();
				System.out.println("BACKEND: client cancelled nested call");
			}
		);
		return ConcurrentInboundObserver.newSimpleConcurrentServerRequestObserver(
			responseObserver,
			maxNestedCallConcurrentRequests,
			(request, individualObserver) -> executor.execute(
				() -> {
					try {
						final var result = processor.process(request);
						individualObserver.onNext(result);
						individualObserver.onCompleted();
					} catch (InterruptedException e) {
						System.out.println("BACKEND: nested processing aborted");
					} catch (Throwable t) {
						processor.abort();
						reportErrorToClient("nested call", t, individualObserver);
						throw t;
					}
				}
			),
			(error, thisObserver) -> {
				processor.abort();
				System.out.println("BACKEND: client cancelled nested call: " + error);
			}
		);
	}



	@Override
	public StreamObserver<ChainedRequest> chained(
			StreamObserver<ChainedResponse> basicResponseObserver) {
		System.out.println("BACKEND: got chained call");
		final var responseObserver =
				(ServerCallStreamObserver<ChainedResponse>) basicResponseObserver;
		final BackendCallProcessor processor = new BackendCallProcessorMock();
		responseObserver.setOnCancelHandler(
			() -> {
				processor.abort();
				System.out.println("BACKEND: client cancelled chained call");
			}
		);
		return new ConcurrentInboundObserver<>(
			responseObserver,
			maxChainedCallConcurrentRequests,
			responseObserver
		) {

			@Override protected void onInboundMessage(
				ChainedRequest request,
				OutboundSubstreamObserver individualObserver
			) {
				executor.execute(
					()-> {
						try {
							final ChainedResponse result = processor.process(request);
							individualObserver.onNext(result);
							individualObserver.onCompleted();
						} catch (IOException e) {
							System.out.println(
									"BACKEND: keep processing but report the error at the end");
							reportErrorAfterAllTasksComplete(
									Status.INTERNAL.withCause(e).asException());
							individualObserver.onCompleted();
						} catch (InterruptedException e) {
							System.out.println("BACKEND: chained processing aborted");
						} catch (Throwable t) {
							processor.abort();
							reportErrorToClient("chained call", t, individualObserver);
							throw t;
						}
					}
				);
			}

			@Override public void onError(Throwable error) {
				processor.abort();
				System.out.println("BACKEND: client cancelled chained call: " + error);
			}
		};
	}



	public static void reportErrorToClient(
		String routine,
		Throwable error,
		CallStreamObserver<?> outboundObserver
	) {
		try {
			System.out.println("BACKEND: error during " + routine
					+ ", forwarding to client: " + error);
			outboundObserver.onError(Status.INTERNAL.withCause(error).asException());
		} catch (StatusRuntimeException ignored) {}
	}



	/**
	 * Domain logic mock.
	 */
	class BackendCallProcessorMock implements BackendCallProcessor {

		final AtomicBoolean aborted = new AtomicBoolean(false);
		private final Set<Thread> activeTasks = ConcurrentHashMap.newKeySet(
			maxNestedCallConcurrentRequests + maxChainedCallConcurrentRequests);



		@Override
		public void abort() {
			aborted.set(true);
			for (final var thread: activeTasks) thread.interrupt();
		}



		@Override
		public NestedResponse process(NestedRequest request) throws InterruptedException {
			return produce(
				activeTasks,
				nestedCallProcessingMillis,
				aborted,
				() -> NestedResponse.newBuilder()
					.setMessageId(request.getMessageId())
					.setMessage(request.getMessage())
					.build()
			);
		}



		final AtomicInteger chainedRequestCounter = new AtomicInteger(0);

		@Override
		public ChainedResponse process(ChainedRequest request)
				throws IOException, InterruptedException {
			if (chainedRequestCounter.incrementAndGet() == sendErrorAt) {
				System.out.println("BACKEND: scheduled error in chained call");
				throw new IOException("scheduled");
			}
			return produce(
				activeTasks,
				chainedCallProcessingMillis,
				aborted,
				() -> ChainedResponse.newBuilder()
					.setMessageId(request.getMessageId())
					.setMessage(request.getMessage())
					.setResultId(request.getResultId())
					.build()
			);
		}
	}



	public BackendServer(
		int port,

		long nestedCallProcessingMillis,
		int maxNestedCallConcurrentRequests,

		long chainedCallProcessingMillis,
		int maxChainedCallConcurrentRequests,
		int sendErrorAt
	) throws IOException {
		this.nestedCallProcessingMillis = nestedCallProcessingMillis;
		this.maxNestedCallConcurrentRequests = maxNestedCallConcurrentRequests;

		this.chainedCallProcessingMillis = chainedCallProcessingMillis;
		this.maxChainedCallConcurrentRequests = maxChainedCallConcurrentRequests;
		this.sendErrorAt = sendErrorAt;

		int numberOfThreads = maxNestedCallConcurrentRequests + maxChainedCallConcurrentRequests;
		executor = new ThreadPoolExecutor(
				numberOfThreads, numberOfThreads, 0L, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		grpcServer = NettyServerBuilder
			.forPort(port)
			.addService(this)
			.addService(ChannelzService.newInstance(1024))
			.build();
		grpcServer.start();
		System.out.println("BACKEND: started on port " + grpcServer.getPort());
		if (sendErrorAt > 0) System.out.println("BACKEND: error scheduled at " + sendErrorAt);
	}



	public int getPort() {
		return grpcServer.getPort();
	}



	public boolean shutdownAndEnforceTermination(long timeoutMillis) throws InterruptedException {
		final var failedTerminations = Awaitable.awaitMultiple(
			timeoutMillis,
			Awaitable.newEntry("grpcServer", GrpcAwaitable.ofEnforcedTermination(grpcServer)),
			Awaitable.newEntry("executor", Awaitable.ofEnforcedTermination(executor))
		);
		for (var failedTermination: failedTerminations) {
			System.out.println("BACKEND: " + failedTermination + " hasn't shutdown cleanly");
		}
		return failedTerminations.isEmpty();
	}



	public static void main(String[] args) throws Exception {
		final int port = args.length > 0 ? Integer.parseInt(args[0]) : 0;

		final long nestedCallProcessingMillis = args.length > 1 ? Long.parseLong(args[1]) : 0L;
		final int maxNestedCallConcurrentRequests = args.length > 3 ? Integer.parseInt(args[3]) : 3;

		final long chainedCallProcessingMillis = args.length > 2 ? Long.parseLong(args[2]) : 0L;
		final int maxChainedCallConcurrentRequests =
				args.length > 4 ? Integer.parseInt(args[4]) : 3;
		final int sendErrorAt = args.length > 3 ? Integer.parseInt(args[3]) : 0;

		final var server = new BackendServer(
			port,

			nestedCallProcessingMillis,
			maxNestedCallConcurrentRequests,

			chainedCallProcessingMillis,
			maxChainedCallConcurrentRequests,
			sendErrorAt
		);

		Runtime.getRuntime().addShutdownHook(new Thread(
			() -> {
				try {
					server.shutdownAndEnforceTermination(500L);
				} catch (InterruptedException ignored) {}
			})
		);
		server.grpcServer.awaitTermination();
	}
}
