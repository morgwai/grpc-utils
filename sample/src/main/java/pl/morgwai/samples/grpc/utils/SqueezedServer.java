// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.samples.grpc.utils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

import io.grpc.*;
import io.grpc.Status.Code;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.stub.*;
import pl.morgwai.base.concurrent.Awaitable;
import pl.morgwai.base.grpc.utils.*;
import pl.morgwai.samples.grpc.utils.BackendGrpc.BackendStub;
import pl.morgwai.samples.grpc.utils.FrontendGrpc.FrontendImplBase;



/**
 * Implements {@code Frontend} service.
 * <ol>
 *   <li>{@link #parent(StreamObserver)} method {@link ParentCallProcessor#preProcess(ParentRequest)
 *     pre-processes} requests in {@link ParentCallProcessor#parentCallRequestObserver} and forwards
 *     them to {@code Backend} service's {@link BackendServer#nested(StreamObserver)} method.</li>
 *   <li>Responses from the nested call are
 *     {@link ParentCallProcessor.ReentrantMidProcessor#midProcess() mid-processed} in
 *     {@link ParentCallProcessor#nestedCallResponseObserver} (producing multiple results) and then
 *     forwarded to {@code Backend} service's {@link BackendServer#chained(StreamObserver)}
 *     method.</li>
 *   <li>Responses from the chained call are {@link ParentCallProcessor#postProcess(ChainedResponse)
 *     post-processed} in {@link ParentCallProcessor#chainedCallResponseObserver} and then forwarded
 *     to parent call's response observer.</li>
 * </ol>
 * <p>
 * Each inbound stream is processed concurrently in multiple threads and resulting outbound messages
 * are produced and sent with respect to flow-control.</p>
 */
public class SqueezedServer extends FrontendImplBase {



	final Server grpcServer;
	final ManagedChannel backendChannel;
	final BackendStub backendConnector;
	final ExecutorService executor;

	final long preProcessingMillis;
	final int maxConcurrentParentRequests;

	final long midProcessingMillis;
	final int maxConcurrentNestedResponses;
	final int midProcessingResultNumber;
	final int midProcessingSubTaskNumber;

	final long postProcessingMillis;
	final int maxConcurrentChainedResponses;



	/**
	 * Domain logic interface.
	 * Instances are created at the beginning of each invocation of
	 * {@link #parent(StreamObserver)} method.
	 * Also holds call's inbound observers ({@link #parentCallRequestObserver},
	 * {@link #nestedCallResponseObserver}, {@link #chainedCallResponseObserver}) for easy
	 * cross-referencing.
	 */
	abstract static class ParentCallProcessor {

		ConcurrentInboundObserver<ParentRequest, NestedRequest, ParentResponse>
				parentCallRequestObserver;
		ConcurrentInboundObserver<NestedResponse, ChainedRequest, NestedRequest>
				nestedCallResponseObserver;
		ConcurrentInboundObserver<ChainedResponse, ParentResponse, ChainedRequest>
				chainedCallResponseObserver;

		abstract void abort();
		abstract NestedRequest preProcess(ParentRequest request) throws InterruptedException;
		abstract ReentrantMidProcessor newReentrantMidProcessor(NestedResponse nestedResponse);
		abstract ParentResponse postProcess(ChainedResponse response) throws InterruptedException;

		interface ReentrantMidProcessor {
			ChainedRequest midProcess() throws InterruptedException;
			boolean hasMoreResults();
		}
	}



	/**
	 * Creates a {@link ParentCallProcessor ParentCallProcessor}, then calls
	 * {@link #createChainedCallResponseObserver(ParentCallProcessor, ServerCallStreamObserver)
	 * createChainedCallResponseObserver(...)} to create
	 * {@link ParentCallProcessor#chainedCallResponseObserver chainedCallResponseObserver}
	 * and issues a chained call.
	 * <p>
	 * This will result in a call to {@code chainedCallResponseObserver.beforeStart(...)} that will
	 * call {@link #createNestedCallResponseObserver(ParentCallProcessor, ServerCallStreamObserver,
	 * ClientCallStreamObserver) createNestedCallResponseObserver(...)} to create
	 * {@link ParentCallProcessor#nestedCallResponseObserver nestedCallResponseObserver} and issue
	 * a nested call.</p>
	 * <p>
	 * This will result in a call to {@code nestedCallResponseObserver.beforeStart(...)} that will
	 * in turn call {@link #createParentCallRequestObserver(ParentCallProcessor,
	 * ServerCallStreamObserver, ClientCallStreamObserver) createParentCallRequestObserver(...)} to
	 * create {@link ParentCallProcessor#parentCallRequestObserver parentCallRequestObserver}, so it
	 * can be returned by this method.</p>
	 */
	@Override
	public StreamObserver<ParentRequest> parent(
			StreamObserver<ParentResponse> responseObserver) {
		System.out.println("SQUEEZED: got call");
		final var parentCallResponseObserver =
				(ServerCallStreamObserver<ParentResponse>) responseObserver;
		final ParentCallProcessor processor = new ParentCallProcessorMock();
		parentCallResponseObserver.setOnCancelHandler(
			() -> {
				processor.abort();
				System.out.println("SQUEEZED: client cancelled, aborting");
			}
		);

		processor.chainedCallResponseObserver =
				createChainedCallResponseObserver(processor, parentCallResponseObserver);
		backendConnector.chained(processor.chainedCallResponseObserver);
		return processor.parentCallRequestObserver;
	}



	/**
	 * Creates {@link ParentCallProcessor#chainedCallResponseObserver chainedCallResponseObserver}.
	 * Called by {@link #parent(StreamObserver) parent(...) RPC method} that passes it to a chained
	 * call.
	 */
	ConcurrentInboundObserver<ChainedResponse, ParentResponse, ChainedRequest>
			createChainedCallResponseObserver(
		ParentCallProcessor processor,
		ServerCallStreamObserver<ParentResponse> parentCallResponseObserver
	) {
		return new ConcurrentInboundObserver<>(
			parentCallResponseObserver,
			maxConcurrentChainedResponses
		) {

			@Override protected void onInboundMessage(
				ChainedResponse chainedResponse,
				OutboundSubstreamObserver individualObserver
			) {
				executor.execute(
					() -> {
						try {
							final var parentResponse = processor.postProcess(chainedResponse);
							individualObserver.onNext(parentResponse);
							individualObserver.onCompleted();
						} catch (InterruptedException e) {
							System.out.println("SQUEEZED: postProcessing aborted");
						} catch (Throwable t) {
							processor.abort();
							reportErrorToParent("postProcessing", t, processor);
							throw t;
						}
					}
				);
			}

			@Override public void onError(Throwable error) {
				if (error instanceof StatusRuntimeException
					&& ((StatusRuntimeException) error).getStatus().getCode() == Code.CANCELLED
				) {
					processor.abort();
					System.out.println("SQUEEZED: chained call was cancelled: " + error);
				} else {
					System.out.println("SQUEEZED: error during chained, waiting for ongoing tasks"
							+ " and forwarding to parent: " + error);
					reportErrorAfterTasksAndInboundComplete(error);
					onCompleted();
				}
			}

			@Override protected void onBeforeStart(
				ClientCallStreamObserver<ChainedRequest> chainedCallRequestObserver) {
				processor.nestedCallResponseObserver = createNestedCallResponseObserver(
						processor, parentCallResponseObserver, chainedCallRequestObserver);
				backendConnector.nested(processor.nestedCallResponseObserver);
			}
		};
	}



	/**
	 * Creates {@link ParentCallProcessor#nestedCallResponseObserver nestedCallResponseObserver}.
	 * Called in {@code chainedCallResponseObserver.beforeStart(...)} and result passed to a nested
	 * call.
 	 */
	ConcurrentInboundObserver<NestedResponse, ChainedRequest, NestedRequest>
			createNestedCallResponseObserver(
		ParentCallProcessor processor,
		ServerCallStreamObserver<ParentResponse> parentCallResponseObserver,
		ClientCallStreamObserver<ChainedRequest> chainedCallRequestObserver
	) {
		return new ConcurrentInboundObserver<>(
			chainedCallRequestObserver,
			maxConcurrentNestedResponses
		) {

			@Override protected void onInboundMessage(
				NestedResponse nestedResponse,
				OutboundSubstreamObserver individualObserver
			) {
				final var midProcessor = processor.newReentrantMidProcessor(nestedResponse);
				DispatchingOnReadyHandler.copyWithFlowControl(
					individualObserver,
					executor,
					midProcessingSubTaskNumber,
					(ignoredTaskNumber) -> midProcessor.hasMoreResults(),
					(ignoredTaskNumber) -> {
						try {
							return midProcessor.midProcess();
						} catch (NoSuchElementException e) {
							throw e;
						} catch (InterruptedException e) {
							System.out.println("SQUEEZED: midProcessing aborted");
							throw new RuntimeException("SQUEEZED: midProcessing aborted");
						} catch (Throwable t) {
							processor.abort();
							reportErrorToParent("midProcessing", t, processor);
							throw t;
						}
					}
				);
			}

			@Override public void onError(Throwable error) {
				processor.abort();
				if (error instanceof StatusRuntimeException
						&& ((StatusRuntimeException) error).getStatus().getCode() == Code.CANCELLED
				) {
					System.out.println("SQUEEZED: nested call was cancelled: " + error);
				} else {
					reportErrorToParent("nested call", error, processor);
				}
			}

			@Override protected void onBeforeStart(
				ClientCallStreamObserver<NestedRequest> nestedCallRequestObserver) {
				processor.parentCallRequestObserver = createParentCallRequestObserver(
						processor, parentCallResponseObserver, nestedCallRequestObserver);
			}
		};
	}



	/**
	 * Creates {@link ParentCallProcessor#parentCallRequestObserver parentCallRequestObserver}.
	 * Called in {@code nestedCallResponseObserver.beforeStart(...)}.
	 * {@code parentCallRequestObserver} will be returned from
	 * {@link #parent(StreamObserver) parent(...) RPC method}.
	 */
	ConcurrentInboundObserver<ParentRequest, NestedRequest, ParentResponse>
			createParentCallRequestObserver(
		ParentCallProcessor processor,
		ServerCallStreamObserver<ParentResponse> parentCallResponseObserver,
		ClientCallStreamObserver<NestedRequest> nestedCallRequestObserver
	) {
		return new ConcurrentInboundObserver<>(
			nestedCallRequestObserver,
			maxConcurrentParentRequests,
			parentCallResponseObserver
		) {

			@Override protected void onInboundMessage(
				ParentRequest parentRequest,
				OutboundSubstreamObserver individualObserver
			) {
				executor.execute(
					() -> {
						try {
							final var nestedRequest = processor.preProcess(parentRequest);
							individualObserver.onNext(nestedRequest);
							individualObserver.onCompleted();
						} catch (InterruptedException e) {
							System.out.println("SQUEEZED: preProcessing aborted");
						} catch (Throwable t) {
							processor.abort();
							reportErrorToParent("preProcessing", t, processor);
							throw t;
						}
					}
				);
			}

			@Override public void onError(Throwable error) {
				processor.abort();
				System.out.println("SQUEEZED: client cancelled, aborting: " + error);
			}
		};
	}



	public static void reportErrorToParent(
		String routine,
		Throwable error,
		ParentCallProcessor processor
	) {
		try {
			System.out.println("SQUEEZED: error during " + routine
					+ ", forwarding to parent: " + error);
			processor.chainedCallResponseObserver.newOutboundSubstream().onError(
					Status.INTERNAL.withCause(error).asException());
		} catch (StatusRuntimeException ignored) {}
	}



	/**
	 * Mocks domain logic processing delay.
	 */
	public static <OutputT> OutputT produce(
		Set<Thread> activeTasks,
		long processingMillis,
		AtomicBoolean aborted,
		Supplier<OutputT> producer
	) throws InterruptedException {
		final var currentThread = Thread.currentThread();
		activeTasks.add(currentThread);
		try {
			if (aborted.get()) currentThread.interrupt();
			Thread.sleep(processingMillis);
			return producer.get();
		} finally {
			activeTasks.remove(currentThread);
		}
	}



	/**
	 * Domain logic mock.
	 */
	class ParentCallProcessorMock extends ParentCallProcessor {

		final AtomicBoolean aborted = new AtomicBoolean(false);
		private final Set<Thread> activeTasks = ConcurrentHashMap.newKeySet(
				maxConcurrentParentRequests
				+ (maxConcurrentNestedResponses * midProcessingSubTaskNumber)
				+ maxConcurrentChainedResponses);



		@Override
		void abort() {
			aborted.set(true);
			for (final var thread: activeTasks) thread.interrupt();
		}



		@Override
		NestedRequest preProcess(ParentRequest request) throws InterruptedException {
			return produce(
				activeTasks,
				preProcessingMillis,
				aborted,
				() -> NestedRequest.newBuilder()
					.setMessageId(request.getMessageId())
					.setMessage(request.getMessage())
					.build()
			);
		}



		@Override
		ParentCallProcessor.ReentrantMidProcessor newReentrantMidProcessor(
				NestedResponse nestedResponse) {
			return new ReentrantMidProcessor(nestedResponse);
		}



		class ReentrantMidProcessor implements ParentCallProcessor.ReentrantMidProcessor {

			final NestedResponse nestedResponse;
			final AtomicInteger resultCounter = new AtomicInteger(0);



			ReentrantMidProcessor(NestedResponse nestedResponse) {
				this.nestedResponse = nestedResponse;
			}



			@Override
			public ChainedRequest midProcess() throws InterruptedException {
				final var resultId = resultCounter.incrementAndGet();
				if (resultId > midProcessingResultNumber) throw new NoSuchElementException();
				return produce(
					activeTasks,
					midProcessingMillis,
					aborted,
					() -> ChainedRequest.newBuilder()
						.setMessageId(nestedResponse.getMessageId())
						.setMessage(nestedResponse.getMessage())
						.setResultId(resultId)
						.build()
				);
			}



			@Override
			public boolean hasMoreResults() {
				return resultCounter.get() <= midProcessingResultNumber;
			}
		}



		@Override
		ParentResponse postProcess(ChainedResponse response) throws InterruptedException {
			return produce(
				activeTasks,
				postProcessingMillis,
				aborted,
				() -> ParentResponse.newBuilder()
					.setMessageId(response.getMessageId())
					.setMessage(response.getMessage())
					.build()
			);
		}
	}



	public SqueezedServer(
		int port,
		String backendTarget,

		long preProcessingMillis,
		int maxConcurrentParentRequests,

		long midProcessingMillis,
		int maxConcurrentNestedResponses,
		int midProcessingResultNumber,
		int midProcessingSubTaskNumber,

		long postProcessingMillis,
		int maxConcurrentChainedResponses
	) throws IOException {
		this.preProcessingMillis = preProcessingMillis;
		this.maxConcurrentParentRequests = maxConcurrentParentRequests;

		this.midProcessingMillis = midProcessingMillis;
		this.maxConcurrentNestedResponses = maxConcurrentNestedResponses;
		this.midProcessingResultNumber = midProcessingResultNumber;
		this.midProcessingSubTaskNumber = midProcessingSubTaskNumber;

		this.postProcessingMillis = postProcessingMillis;
		this.maxConcurrentChainedResponses = maxConcurrentChainedResponses;

		final var threadNumber =
				maxConcurrentParentRequests
				+ (maxConcurrentNestedResponses * midProcessingSubTaskNumber)
				+ maxConcurrentChainedResponses;
		executor = new ThreadPoolExecutor(
				threadNumber, threadNumber, 0L, TimeUnit.DAYS, new LinkedBlockingQueue<>());
		backendChannel = ManagedChannelBuilder
			.forTarget(backendTarget)
			.usePlaintext()
			.build();
		backendConnector = BackendGrpc.newStub(backendChannel);
		grpcServer = NettyServerBuilder
			.forPort(port)
			.addService(this)
			.addService(ChannelzService.newInstance(1024))
			.build();
		grpcServer.start();
		System.out.println("SQUEEZED: started on port " + grpcServer.getPort());
	}



	public int getPort() {
		return grpcServer.getPort();
	}



	public boolean shutdownAndEnforceTermination(long timeoutMillis) throws InterruptedException {
		final var failedTerminations = Awaitable.awaitMultiple(
			timeoutMillis,
			Awaitable.entry("grpcServer", GrpcAwaitable.ofEnforcedTermination(grpcServer)),
			Awaitable.entry("executor", Awaitable.ofEnforcedTermination(executor)),
			Awaitable.entry("backendChannel", GrpcAwaitable.ofEnforcedTermination(backendChannel))
		);
		for (var failedTermination: failedTerminations) {
			System.out.println("SQUEEZED: " + failedTermination + " hasn't shutdown cleanly");
		}
		return failedTerminations.isEmpty();
	}



	public static void main(String[] args) throws Exception {
		final long preProcessingMillis = args.length > 2 ? Long.parseLong(args[2]) : 0L;
		final int maxConcurrentParentRequests = args.length > 3 ? Integer.parseInt(args[3]) : 3;

		final long midProcessingMillis = args.length > 4 ? Long.parseLong(args[4]) : 0L;
		final int maxConcurrentNestedResponses = args.length > 5 ? Integer.parseInt(args[5]) : 3;
		final int midProcessingResultNumber = args.length > 6 ? Integer.parseInt(args[6]) : 9;
		final int midProcessingSubTaskNumber = args.length > 7 ? Integer.parseInt(args[7]) : 3;

		final long postProcessingMillis = args.length > 8 ? Long.parseLong(args[8]) : 0L;
		final int maxConcurrentChainedResponses = args.length > 9 ? Integer.parseInt(args[7]) : 9;

		final var server = new SqueezedServer(
			Integer.parseInt(args[0]),
			args[1],

			preProcessingMillis,
			maxConcurrentParentRequests,

			midProcessingMillis,
			maxConcurrentNestedResponses,
			midProcessingResultNumber,
			midProcessingSubTaskNumber,

			postProcessingMillis,
			maxConcurrentChainedResponses
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
