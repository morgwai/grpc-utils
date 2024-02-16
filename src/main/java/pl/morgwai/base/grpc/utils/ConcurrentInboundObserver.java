// Copyright 2022 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



/**
 * Base class for inbound {@link StreamObserver}s (server method
 * {@link #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer, BiConsumer,
 * ServerCallStreamObserver) request observers} and {@link #newConcurrentClientResponseObserver(
 * CallStreamObserver, int, BiConsumer, BiConsumer, Consumer) client response observers}), that pass
 * results to some {@link CallStreamObserver outboundObserver} and may dispatch message processing
 * to other threads.
 * Handles all the synchronization and manual flow-control to maintain the number of messages
 * processed concurrently configured with
 * {@code maxConcurrentRequestMessages}&nbsp;/&nbsp;{@code maxConcurrentClientResponseMessages}
 * constructor param.
 * <p>
 * Each inbound message is assigned a separate dedicated {@link SubstreamObserver observer
 * of the substream of outbound messages} resulting from processing of this inbound message. Inbound
 * messages together with their dedicated results observers are passed as arguments to
 * {@link #onInboundMessage(Object, SubstreamObserver)
 * onInboundMessage(message, messageResultsDedicatedObserver)} method that must either be overridden
 * in a subclass or its handler must supplied on
 * {@code onServerRequestHandler}&nbsp;/&nbsp;{@code onClientResponseHandler} constructor param.</p>
 * <p>
 * If an application needs to send additional outbound messages not related to any inbound message,
 * it can create additional outbound substream observers using {@link #newOutboundSubstream()}.<br/>
 * Once all outbound substream observers are marked as completed and the inbound stream is closed by
 * the remote peer, {@link CallStreamObserver#onCompleted() outboundObserver.onCompleted()} is
 * called automatically.</p>
 * <p>
 * If some additional action is required once the inbound stream is closed by the remote peer,
 * {@link #onHalfClosed()} method may be overridden or its handler may be set using
 * {@link #setOnHalfClosedHandler(Runnable)} method.</p>
 * <p>
 * If work is not dispatched to other threads, then processing of inbound messages will be
 * sequential and performed with respect to flow-control. Unless significant message delivery delay
 * is expected {@code maxConcurrent*Messages} constructor param should usually be set to {@code 1}
 * in such cases.</p>
 * <p>
 * If work is dispatched to other threads, resulting outbound messages may be sent in a different
 * order than the inbound messages arrived: see {@link OrderedConcurrentInboundObserver} if the
 * order needs to be preserved.</p>
 * <p>
 * If processing of one inbound message may produce multiple outbound messages,
 * {@link #onInboundMessage(Object, SubstreamObserver) onInboundMessage(...)} may pass its dedicated
 * observer to utilities like
 * {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)
 * StreamObservers.copyWithFlowControl(...)} in case of sequential processing or
 * {@link DispatchingOnReadyHandler#copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
 * DispatchingOnReadyHandler.copyWithFlowControl(...)} in case work needs to be dispatched to other
 * threads.</p>
 * <p>
 * Instances can be created either using "named constructor" static methods
 * ({@link #newSimpleConcurrentServerRequestObserver(ServerCallStreamObserver, int, BiConsumer,
 * BiConsumer) newSimpleConcurrentServerRequestObserver(...)},
 * {@link #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer, BiConsumer,
 * ServerCallStreamObserver) newConcurrentServerRequestObserver(...)},
 * {@link #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
 * Consumer) newConcurrentClientResponseObserver(...)}&nbsp;) that accept functional arguments for
 * method handlers or by creating anonymous subclasses using protected constructors
 * ({@link #ConcurrentInboundObserver(CallStreamObserver, int, ServerCallStreamObserver) server
 * request variant} and {@link #ConcurrentInboundObserver(CallStreamObserver, int) client response
 * variant}) and overriding the desired methods.</p>
 *
 * @see OrderedConcurrentInboundObserver
 */
public class ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		implements ClientResponseObserver<ControlT, InboundT> {



	/**
	 * Thread-safe substream of an {@link CallStreamObserver outbound observer}.
	 * See {@link ConcurrentInboundObserver.OutboundSubstreamObserver} for details.
	 */
	public static abstract class SubstreamObserver<MessageT> extends CallStreamObserver<MessageT> {

		/** Calls {@link ConcurrentInboundObserver#reportErrorAfterAllTasksComplete(Throwable)}. */
		public abstract void reportErrorAfterAllTasksComplete(Throwable errorToReport);

		/** Calls {@link ConcurrentInboundObserver#newOutboundSubstream()}. */
		public abstract SubstreamObserver<MessageT> newOutboundSubstream();
	}



	/**
	 * Creates a server method request observer for gRPC methods that don't issue any nested client
	 * calls and pass results directly to method's response observer.
	 * Configures flow-control and initializes handlers {@link #onInboundMessageHandler} and
	 * {@link #onErrorHandler}.
	 * <p>
	 * Example:</p>
	 * <pre>{@code
	 * public StreamObserver<Request> biDiRpc(StreamObserver<Response> basicResponseObserver) {
	 *     final var responseObserver = (ServerCallStreamObserver<Response>) basicResponseObserver;
	 *     responseObserver.setOnCancelHandler(() -> {
	 *             // abort ongoing tasks if needed
	 *     });
	 *     return newSimpleConcurrentServerRequestObserver(
	 *         responseObserver,
	 *         MAX_CONCURRENTLY_PROCESSED_REQUESTS,
	 *         (request, responseSubstreamObserver) -> executor.execute(
	 *             () -> {
	 *                 final var response = process(request);
	 *                 responseSubstreamObserver.onNext(response);
	 *                 responseSubstreamObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -> {}
	 *     );
	 * }}</pre>
	 *
	 * @param responseObserver response observer of the given server method.
	 * @param maxConcurrentRequestMessages the constructed observer will initially call
	 *     {@link CallStreamObserver#request(int)
	 *     responseObserver.request(maxConcurrentServerRequestMessages)}. If the request
	 *     processing is dispatched to other threads, this will in consequence be the maximum number
	 *     of request messages processed concurrently. It should correspond to the server processing
	 *     capabilities.
	 * @param onRequestHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, SubstreamObserver)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 *
	 * @see #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer,
	 *     BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...) in case the
	 *     server method needs to issue some nested client calls.
	 */
	public static <RequestT, ResponseT>
	ConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
	newSimpleConcurrentServerRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int maxConcurrentRequestMessages,
		BiConsumer<? super RequestT, ? super SubstreamObserver<ResponseT>> onRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
				> onErrorHandler
	) {
		return new ConcurrentInboundObserver<>(
			responseObserver,
			maxConcurrentRequestMessages,
			onRequestHandler,
			onErrorHandler,
			responseObserver
		);
	}



	/**
	 * Creates a server method request observer for gRPC methods that issue nested client calls.
	 * Configures flow-control and initializes handlers {@link #onInboundMessageHandler} and
	 * {@link #onErrorHandler}.
	 * <p>
	 * Example:</p>
	 * <pre>{@code
	 * public StreamObserver<ServerRequest> frontendRpc(
	 *     StreamObserver<ServerResponse> responseObserver
	 * ) {
	 *     final var serverResponseObserver =
	 *             (ServerCallStreamObserver<ServerResponse>) responseObserver;
	 *     serverResponseObserver.setOnCancelHandler(() -> {});
	 *     final StreamObserver<ServerRequest>[] serverRequestObserverHolder = {null};
	 *     backendStub.nestedRpc(newConcurrentClientResponseObserver(
	 *         serverResponseObserver,
	 *         MAX_CONCURRENTLY_PROCESSED_NESTED_RESPONSES,
	 *         (nestedResponse, serverResponseSubstreamObserver) ->  executor.execute(
	 *             () -> {
	 *                 final var serverResponse = postProcess(nestedResponse);
	 *                 serverResponseSubstreamObserver.onNext(frontendResponse);
	 *                 serverResponseSubstreamObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -> {
	 *             thisObserver.reportErrorAfterTasksAndInboundComplete(error);
	 *             thisObserver.onCompleted();
	 *         },
	 *         (nestedRequestObserver) -> {
	 *             serverRequestObserverHolder[0] = newConcurrentServerRequestObserver(
	 *                 nestedRequestObserver,
	 *                 MAX_CONCURRENTLY_PROCESSED_OUTER_REQUESTS,
	 *                 (serverRequest, nestedRequestSubstreamObserver) -> executor.execute(
	 *                     () -> {
	 *                         final var nestedRequest = preProcess(serverRequest);
	 *                         nestedRequestSubstreamObserver.onNext(nestedRequest);
	 *                         nestedRequestSubstreamObserver.onCompleted();
	 *                     }
	 *                 ),
	 *                 (error, thisObserver) -> {
	 *                     // client cancelled, abort tasks if needed
	 *                 },
	 *                 serverResponseObserver
	 *             );
	 *         }
	 *     ));
	 *     return serverRequestObserverHolder[0];
	 * }}</pre>
	 *
	 * @param nestedRequestObserver request observer of the nested client call to which results
	 *     should be streamed
	 * @param maxConcurrentServerRequestMessages the constructed observer will initially call
	 *     {@link CallStreamObserver#request(int)
	 *     serverResponseObserver.request(maxConcurrentServerRequestMessages)}. If the request
	 *     processing is dispatched to other threads, this will in consequence be the maximum number
	 *     of request messages processed concurrently. It should correspond to the server processing
	 *     capabilities.
	 * @param onServerRequestHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, SubstreamObserver)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param serverResponseObserver server response observer of the given RPC method used for
	 *     inbound control: {@link ServerCallStreamObserver#disableAutoInboundFlowControl()
	 *     serverResponseObserver.disableAutoInboundFlowControl()} will be called once at the
	 *     beginning and {@link ServerCallStreamObserver#request(int)
	 *     serverResponseObserver.request(k)} several times throughout the lifetime of the given RPC
	 *     call processing.
	 *
	 * @see #newSimpleConcurrentServerRequestObserver(ServerCallStreamObserver, int, BiConsumer,
	 *     BiConsumer) newSimpleConcurrentServerRequestObserver(...) in case no nested calls are
	 *     issued.
	 * @see #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     Consumer) newConcurrentClientResponseObserver(...) for creating response observers
	 *     for nested calls.
	 */
	public static <ServerRequestT, NestedRequestT, ServerResponseT>
	ConcurrentInboundObserver<ServerRequestT, NestedRequestT, ServerResponseT>
	newConcurrentServerRequestObserver(
		ClientCallStreamObserver<NestedRequestT> nestedRequestObserver,
		int maxConcurrentServerRequestMessages,
		BiConsumer<? super ServerRequestT, ? super SubstreamObserver<NestedRequestT>>
				onServerRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<
							ServerRequestT, NestedRequestT, ServerResponseT>
				> onErrorHandler,
		ServerCallStreamObserver<ServerResponseT> serverResponseObserver
	) {
		return new ConcurrentInboundObserver<>(
			nestedRequestObserver,
			maxConcurrentServerRequestMessages,
			onServerRequestHandler,
			onErrorHandler,
			serverResponseObserver
		);
	}



	/**
	 * Creates a client call response observer.
	 * Configures flow-control and initializes handlers {@link #onInboundMessageHandler},
	 * {@link #onErrorHandler} and {@link #onBeforeStartHandler}.
	 * The created observer may either be forwarding results to the request observer of another
	 * "chained" client call, or to the response observer of its parent server call, if the client
	 * call is nested within some server call.
	 * <p>
	 * Forwarding results to another "chained" client call from a top-level client example:</p>
	 * <pre>{@code
	 * stub.chainedCall(new ClientResponseObserver<ChainedRequest, ChainedResponse>() {
	 *
	 *     public void beforeStart(ClientCallStreamObserver<ChainedRequest> chainedRequestObserver)
	 *     {
	 *         anotherStub.headCall(newConcurrentClientResponseObserver(
	 *             chainedRequestObserver,
	 *             MAX_CONCURRENTLY_PROCESSED_HEAD_CALL_RESPONSES,
	 *             (headCallResponse, chainedRequestSubstreamObserver) -> executor.execute(
	 *                 () -> {
	 *                     final var chainedRequest = midProcess(headCallResponse);
	 *                     chainedRequestSubstreamObserver.onNext(chainedRequest);
	 *                     chainedRequestSubstreamObserver.onCompleted();
	 *                 }
	 *             ),
	 *             (error, thisObserver) -> {
	 *                 thisObserver.reportErrorAfterTasksAndInboundComplete(error);
	 *                 thisObserver.onCompleted();
	 *             },
	 *             (ClientCallStreamObserver<HeadCallRequest> headCallRequestObserver) ->
	 *                     copyWithFlowControl(headCallRequestProducer, headCallRequestObserver)
	 *         ));
	 *     }
	 *
	 *     public void onNext(ChainedResponse chainedResponse) {
	 *         // display chainedResponse on the UI
	 *     }
	 *     public void onError(Throwable error) {
	 *         // display some error message on the UI
	 *     }
	 *     public void onCompleted() {
	 *         // display some completion message on the UI
	 *     }
	 * });}</pre>
	 * <p>
	 * See {@link #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...)} for a nested
	 * call example.</p>
	 *
	 * @param outboundObserver either the request observer of another "chained" client call or the
	 *     response observer of the parent server call that the observer being created is nested in.
	 * @param maxConcurrentClientResponseMessages the constructed observer will call
	 *     {@link CallStreamObserver#request(int)} on the call's request observer obtained via
	 *     {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)}.
	 *     If the response processing is dispatched to other threads, this will in consequence be
	 *     the maximum number of response messages processed concurrently. It should correspond to
	 *     the client processing capabilities.
	 * @param onClientResponseHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, SubstreamObserver)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param onBeforeStartHandler stored on {@link #onBeforeStartHandler} to be called by
	 *     {@link #onBeforeStart(ClientCallStreamObserver)} called by
	 *     {@link #beforeStart(ClientCallStreamObserver)}.
	 */
	public static <ClientResponseT, OutboundT, ClientRequestT>
	ConcurrentInboundObserver<ClientResponseT, OutboundT, ClientRequestT>
	newConcurrentClientResponseObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentClientResponseMessages,
		BiConsumer<? super ClientResponseT, ? super SubstreamObserver<OutboundT>>
				onClientResponseHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<ClientResponseT, OutboundT, ClientRequestT>
				> onErrorHandler,
		Consumer<? super ClientCallStreamObserver<ClientRequestT>> onBeforeStartHandler
	) {
		return new ConcurrentInboundObserver<>(
			outboundObserver,
			maxConcurrentClientResponseMessages,
			onClientResponseHandler,
			onErrorHandler,
			onBeforeStartHandler
		);
	}



	/**
	 * Creates a server method request observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. At least
	 * {@link #onInboundMessage(Object, SubstreamObserver) onInboundMessage(...)} must be
	 * overridden.
	 * @see #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer,
	 *     BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...) for
	 *     descriptions of the params.
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentServerRequestMessages,
		ServerCallStreamObserver<ControlT> serverResponseObserver
	) {
		this(
			outboundObserver,
			maxConcurrentServerRequestMessages,
			null,
			null,
			serverResponseObserver
		);
	}

	/**
	 * Creates a client response observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. At least
	 * {@link #onInboundMessage(Object, SubstreamObserver) onInboundMessage(...)} must be
	 * overridden.
	 * @see #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     Consumer) newConcurrentClientResponseObserver(...) for descriptions of the params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentClientResponseMessages
	) {
		this(
			outboundObserver,
			maxConcurrentClientResponseMessages,
			null,
			null,
			(Consumer<? super ClientCallStreamObserver<ControlT>>) null
		);
	}



	/**
	 * Called by {@link #onNext(Object) onNext(message)}, processes {@code message}.
	 * Resulting outbound messages must be passed to {@code messageResultsDedicatedObserver}
	 * associated with this given {@code message} using {@link SubstreamObserver#onNext(Object)
	 * messageResultsDedicatedObserver.onNext(resultOutboundMessage)}. Once the processing is done
	 * and all the resulting outbound messages are submitted, either
	 * {@link SubstreamObserver#onCompleted() messageResultsDedicatedObserver.onCompleted()} or
	 * {@link SubstreamObserver#onError(Throwable) messageResultsDedicatedObserver.onError(...)}
	 * must be called.
	 * <p>
	 * {@code messageResultsDedicatedObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * If processing of one {@code message} may produce multiple outbound messages,
	 * this method should respect {@code messageResultsDedicatedObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() messageResultsDedicatedObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * messageResultsDedicatedObserver.setOnReadyHandler(...)} methods to avoid excessive
	 * buffering. Consider using {@link DispatchingOnReadyHandler} in case work must be dispatched
	 * to other threads or {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}
	 * otherwise:</p>
	 * <pre>{@code
	 * class RequestProcessor implements Iterator<Response> {
	 *     RequestProcessor(RequestMessage request) { ... }
	 *     public boolean hasNext() { ... }
	 *     public Response next() { ... }
	 * }
	 *
	 * public StreamObserver<Request> myBiDiMethod(StreamObserver<Response> basicResponseObserver) {
	 *     final var responseObserver = (ServerCallStreamObserver<Response>) basicResponseObserver;
	 *     return newSimpleConcurrentServerRequestObserver(
	 *         responseObserver,
	 *         1,
	 *         (request, responseSubstreamObserver) -> StreamObservers.copyWithFlowControl(
	 *             new RequestProcessor(request),
	 *             responseSubstreamObserver
	 *         ),
	 *         (error, thisObserver) -> responseObserver.onError(error);
	 *     );
	 * }}</pre>
	 * <p>
	 * The default implementation calls {@link #onInboundMessageHandler}.</p>
	 *
	 * @see SubstreamObserver
	 */
	protected void onInboundMessage(
		InboundT message,
		SubstreamObserver<OutboundT> messageResultsDedicatedObserver
	) {
		onInboundMessageHandler.accept(message, messageResultsDedicatedObserver);
	}

	/**
	 * Called by the default implementation of
	 * {@link #onInboundMessage(Object, SubstreamObserver)
	 * onInboundMessage(...)}. Initialized via {@code onInboundMessageHandler} param of
	 * {@link #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...)},
	 * {@link #newSimpleConcurrentServerRequestObserver(ServerCallStreamObserver, int, BiConsumer,
	 * BiConsumer) newSimpleConcurrentServerRequestObserver(...)} and
	 * {@link #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * Consumer) newConcurrentClientResponseObserver(...)} "constructors".
	 */
	protected final BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>>
			onInboundMessageHandler;



	/**
	 * The default implementation calls {@link #onErrorHandler} if it is not {@code null}.
	 * @see StreamObserver#onError(Throwable)
	 */
	@Override
	public void onError(Throwable error) {
		if (onErrorHandler != null) onErrorHandler.accept(error, this);
	}

	/**
	 * Called by the default implementation of {@link #onError(Throwable)}.
	 * Initialized via {@code onErrorHandler} param of
	 * {@link #newConcurrentServerRequestObserver(ClientCallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...)},
	 * {@link #newSimpleConcurrentServerRequestObserver(ServerCallStreamObserver, int, BiConsumer,
	 * BiConsumer) newSimpleConcurrentServerRequestObserver(...)} and
	 * {@link #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * Consumer) newConcurrentClientResponseObserver(...)} "constructors".
	 * The second parameter is a reference to the calling inbound observer, it may be used to call
	 * {@link #reportErrorAfterAllTasksComplete(Throwable)} or {@link #onCompleted()}.
	 */
	protected final BiConsumer<
				? super Throwable,
				? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
			> onErrorHandler;



	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver) beforeStart(inboundControlObserver)},
	 * the default implementation calls {@link #onBeforeStartHandler} if it is not {@code null}.
	 * This method is meaningful only in client response observers in which case it is called by
	 * {@link #beforeStart(ClientCallStreamObserver)} method. It is never called in case of server
	 * request observers.
	 * @see ClientResponseObserver#beforeStart(ClientCallStreamObserver)
	 */
	protected void onBeforeStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		if (onBeforeStartHandler != null) onBeforeStartHandler.accept(inboundControlObserver);
	}

	/**
	 * Called by the default implementation of {@link #onBeforeStart(ClientCallStreamObserver)
	 * beforeStart(inboundControlObserver)}. Initialized via {@code onBeforeStartHandler} param of
	 * {@link #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * Consumer) newConcurrentClientResponseObserver(...)} "constructor".
	 */
	protected final Consumer<? super ClientCallStreamObserver<ControlT>> onBeforeStartHandler;



	/**
	 * Indicates that after all tasks and the inbound stream are completed, {@code errorToReport}
	 * should be nevertheless reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(errorToReport)}.
	 * <p>
	 * If after this method is called, any of the remaining outbound substream observers gets a call
	 * to its {@link SubstreamObserver#onError(Throwable) onError(errror)}, then
	 * {@code errorToReport} will be discarded.</p>
	 * <p>
	 * If this method is called from this observer's {@link #onError(Throwable)}, it should be
	 * followed by {@link #onCompleted()} to manually mark inbound stream as completed
	 * (half-close).<br/>
	 * If it's called in {@link #onInboundMessage(Object, SubstreamObserver) onInboundMessage(...)},
	 * it should be eventually followed by
	 * {@link SubstreamObserver#onCompleted() dedicatedObserver.onCompleted()}.</p>
	 */
	public final void reportErrorAfterAllTasksComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
	}



	/**
	 * Creates a new {@link SubstreamObserver outbound substream} not related to any inbound
	 * message.
	 * The parent {@code outboundObserver} will not be marked as completed until all substreams
	 * (either created manually with this method or automatically for collecting results of inbound
	 * messages processing) are {@link SubstreamObserver#onCompleted() completed}.
	 * <p>
	 * <b>NOTE:</b> after creating a substream with this method and
	 * {@link SubstreamObserver#setOnReadyHandler(Runnable) setting its onReadyHandler}, it may be
	 * necessary to manually run it the first time. Both setting the handler and its first manual
	 * run, may <b>only</b> be performed on a gRPC {@code Thread} bound by the gRPC concurrency
	 * contract to ensure consistency of {@code onReady()} calls to the set handler.</p>
	 */
	public final SubstreamObserver<OutboundT> newOutboundSubstream() {
		return newOutboundSubstream(false);
	}



	/**
	 * Called at the beginning of {@link #onCompleted()}. The default implementation calls
	 * {@link #onHalfClosedHandler} if it is not {@code null}.
	 */
	protected void onHalfClosed() {
		if (onHalfClosedHandler != null) onHalfClosedHandler.run();
	}

	/** Sets {@link #onHalfClosedHandler}. */
	public void setOnHalfClosedHandler(Runnable onHalfClosedHandler) {
		this.onHalfClosedHandler = onHalfClosedHandler;
	}

	/**
	 * Called by the default implementation of {@link #onHalfClosed()} if not {@code null}. May be
	 * set using {@link #setOnHalfClosedHandler(Runnable)}.
	 */
	protected Runnable onHalfClosedHandler;



	final CallStreamObserver<OutboundT> outboundObserver;

	CallStreamObserver<?> inboundControlObserver; // for request(n)
	boolean halfClosed = false;
	int idleCount;  // increased if outbound unready after completing a substream from onNext(...)
	final Set<OutboundSubstreamObserver> activeOutboundSubstreams = ConcurrentHashMap.newKeySet();
	Throwable errorToReport;

	final Object lock = new Object();



	/**
	 * Low level constructor for derived libs' server method request observers.
	 * App-level code should instead use {@link #newSimpleConcurrentServerRequestObserver(
	 * ServerCallStreamObserver, int, BiConsumer, BiConsumer)
	 * newSimpleConcurrentServerRequestObserver(...)} or {@link #newConcurrentServerRequestObserver(
	 * ClientCallStreamObserver, int, BiConsumer, BiConsumer, ServerCallStreamObserver)
	 * newConcurrentServerRequestObserver(...)} or
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, ServerCallStreamObserver)}.
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentServerRequestMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onServerRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<ControlT> serverResponseObserver
	) {
		this(
			outboundObserver,
			maxConcurrentServerRequestMessages,
			onServerRequestHandler,
			onErrorHandler,
			(Consumer<? super ClientCallStreamObserver<ControlT>>) null
		);
		setInboundControlObserver(serverResponseObserver);
	}



	/**
	 * Lowest level constructor that performs actual instance initialization.
	 * Serves also as a low level constructor for derived libs' client response observers.
	 * App-level code should instead use {@link #newConcurrentClientResponseObserver(
	 * CallStreamObserver, int, BiConsumer, BiConsumer, Consumer)
	 * newConcurrentClientResponseObserver(...)} or
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int)}.
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentInboundMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		Consumer<? super ClientCallStreamObserver<ControlT>> onBeforeStartHandler
	) {
		this.outboundObserver = outboundObserver;
		idleCount = maxConcurrentInboundMessages;
		this.onInboundMessageHandler = onInboundMessageHandler;
		this.onErrorHandler = onErrorHandler;
		this.onBeforeStartHandler = onBeforeStartHandler;
		outboundObserver.setOnReadyHandler(this::onReady);
	}



	/**
	 * Called in {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * ServerCallStreamObserver) constructors} in case of server method request observers and in
	 * {@link #beforeStart(ClientCallStreamObserver)} in case of client response observers.
	 */
	final void setInboundControlObserver(CallStreamObserver<?> inboundControlObserver) {
		if (this.inboundControlObserver != null) {
			throw new IllegalStateException("inboundControlObserver already set");
		}
		this.inboundControlObserver = inboundControlObserver;
		inboundControlObserver.disableAutoInboundFlowControl();
	}



	/**
	 * Sets {@code clientCallRequestObserver} as the inbound control and calls
	 * {@link #onBeforeStart(ClientCallStreamObserver)}.
	 * This method is meaningful only in case of client response observers, it is never called in
	 * case of server request observers.
	 * @see ClientResponseObserver#beforeStart(ClientCallStreamObserver)
	 */
	@Override
	public final void beforeStart(ClientCallStreamObserver<ControlT> clientCallRequestObserver) {
		setInboundControlObserver(clientCallRequestObserver);
		onBeforeStart(clientCallRequestObserver);
	}



	/**
	 * Calls {@link OutboundSubstreamObserver#onReady() onReadyHandlers} of the
	 * {@link #activeOutboundSubstreams active substream observers} and
	 * {@link CallStreamObserver#request(int) requests} a number of inbound messages need to achieve
	 * concurrency level set with {@code maxConcurrentInboundMessages}
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer, Consumer)
	 * constructor} param.
	 */
	final void onReady() {
		synchronized (lock) {
			if (idleCount > 0 && !halfClosed) {
				inboundControlObserver.request(idleCount);
				idleCount = 0;
			}
			// TODO: add routines for checking processing resources availability
		}
		for (var substreamObserver: activeOutboundSubstreams) {
			// a new inbound can't arrive now due to Listener's concurrency contract and
			// activeOutboundSubstreams is a concurrent Set, so removals in substream.onCompleted()
			// are not a problem (some completed substreams may receive onReady() though)
			substreamObserver.onReady();
		}
	}



	/**
	 * Calls {@link #onInboundMessage(Object, SubstreamObserver)
	 * onInboundMessage}({@code message}, {@link #newOutboundSubstream()}) and if the parent
	 * {@code outboundObserver} is ready, then also {@link OutboundSubstreamObserver#onReady()
	 * messageResultsDedicatedObserver.onReadyHandler}.
	 */
	@Override
	public final void onNext(InboundT message) {
		final var messageResultsDedicatedObserver = newOutboundSubstream(true);
		onInboundMessage(message, messageResultsDedicatedObserver);
		messageResultsDedicatedObserver.lockOnReadyHandler();
		synchronized (lock) {
			if ( !outboundObserver.isReady()) return;
		}
		messageResultsDedicatedObserver.onReady();
	}



	/**
	 * Calls {@link #onHalfClosed()}, marks the inbound as completed and if all
	 * {@link #newOutboundSubstream() outbound substreams} are marked as completed, then marks
	 * the parent {@code outboundObserver} as completed also.
	 */
	@Override
	public final void onCompleted() {
		onHalfClosed();
		synchronized (lock) {
			halfClosed = true;
			if (activeOutboundSubstreams.isEmpty()) {
				if (errorToReport != null) {
					outboundObserver.onError(errorToReport);
				} else {
					outboundObserver.onCompleted();
				}
			}
		}
	}



	/**
	 * Creates a new {@link OutboundSubstreamObserver outbound substream}.
	 * This method is called automatically to create a new substream for each
	 * {@link #onInboundMessage(Object, SubstreamObserver) inbound message} and may be called
	 * manually by user code via {@link #newOutboundSubstream()} to create additional substreams.
	 * <p>
	 * Subclasses may override this method if they need to use specialized subclasses of
	 * {@link OutboundSubstreamObserver OutboundSubstreamObserver}: see
	 * {@link OrderedConcurrentInboundObserver#newOutboundSubstream(boolean)} for an example.</p>
	 *
	 * @param requestNextAfterCompletion passed to
	 *     {@link OutboundSubstreamObserver#OutboundSubstreamObserver(boolean)}.
	 */
	protected OutboundSubstreamObserver newOutboundSubstream(boolean requestNextAfterCompletion) {
		return new OutboundSubstreamObserver(requestNextAfterCompletion);
	}



	/**
	 * A thread-safe observer of a substream of the parent outbound stream.
	 * The parent {@code outboundObserver} will be marked as
	 * {@link CallStreamObserver#onCompleted() completed} automatically when and only when all of
	 * its {@link StreamObserver#onCompleted() substreams} and
	 * {@link #onCompleted() the inbound stream} are marked as completed.
	 * <p>
	 * <b>NOTE:</b> {@link OutboundSubstreamObserver#setOnReadyHandler(Runnable)} may only be called
	 * on a gRPC {@code Thread} bound by the gRPC concurrency contract.</p>
	 *
	 * @see #newOutboundSubstream()
	 * @see #newOutboundSubstream(boolean)
	 */
	protected class OutboundSubstreamObserver extends SubstreamObserver<OutboundT> {

		Runnable onReadyHandler;
		boolean onReadyHandlerLocked = false;

		final boolean requestNextAfterCompletion;



		/**
		 * Constructs a new substream.
		 * @param requestNextAfterCompletion if {@code true}, then upon
		 *     {@link #onCompleted() completion} of this substream, a next inbound message will be
		 *     {@link CallStreamObserver#request(int) requested} automatically. This should be set
		 *     to {@code false} for additional user-created substreams.
		 */
		protected OutboundSubstreamObserver(boolean requestNextAfterCompletion) {
			this.requestNextAfterCompletion = requestNextAfterCompletion;
			activeOutboundSubstreams.add(this);
		}



		/**
		 * Marks this substream as completed.
		 * When all outbound substreams and the inbound stream are completed, the parent
		 * {@code outboundObserver} will be
		 * {@link CallStreamObserver#onCompleted() marked as completed} automatically.
		 */
		@Override
		public void onCompleted() {
			synchronized (lock) {
				if ( !activeOutboundSubstreams.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (halfClosed && activeOutboundSubstreams.isEmpty()) {
					if (errorToReport != null) {
						outboundObserver.onError(errorToReport);
					} else {
						outboundObserver.onCompleted();
					}
					return;
				}

				if (requestNextAfterCompletion) {
					if (outboundObserver.isReady()) {
						inboundControlObserver.request(1);
					} else {
						idleCount++;
					}
				}
			}
		}



		/** Forwards {@code message} to the parent {@code outboundObserver}. */
		@Override
		public void onNext(OutboundT message) {
			synchronized (lock) {
				if ( !activeOutboundSubstreams.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				outboundObserver.onNext(message);
			}
		}



		/** Forwards {@code error} to the parent {@code outboundObserver}. */
		@Override
		public void onError(Throwable error) {
			synchronized (lock) {
				// don't remove to prevent others from doing outboundObserver.onCompleted()
				if ( !activeOutboundSubstreams.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				outboundObserver.onError(error);
			}
		}



		@Override
		public boolean isReady() {
			synchronized (lock) {
				return outboundObserver.isReady();
			}
		}



		void lockOnReadyHandler() {
			onReadyHandlerLocked = true;
		}



		/**
		 * Sets {@code onReadyHandler} to be called when this observer becomes ready.
		 * This method may only be called on a gRPC {@code Thread} bound by the gRPC concurrency
		 * contract to ensure consistency of {@code onReady()} calls to {@code onReadyHandler}.
		 */
		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			if (onReadyHandlerLocked) throw new IllegalStateException("onReadyHandler locked");
			this.onReadyHandler = onReadyHandler;
		}



		/** Calls the handler set via {@link #setOnReadyHandler(Runnable)} if any. */
		void onReady() {
			onReadyHandlerLocked = true;
			if (onReadyHandler != null) onReadyHandler.run();
		}



		/** Calls {@link ConcurrentInboundObserver#reportErrorAfterAllTasksComplete(Throwable)}. */
		@Override
		public final void reportErrorAfterAllTasksComplete(Throwable errorToReport) {
			ConcurrentInboundObserver.this.reportErrorAfterAllTasksComplete(errorToReport);
		}



		/** Calls {@link ConcurrentInboundObserver#newOutboundSubstream()}. */
		@Override
		public SubstreamObserver<OutboundT> newOutboundSubstream() {
			return ConcurrentInboundObserver.this.newOutboundSubstream();
		}



		// only unsupported operations and internal constants below

		/** Throws {@link UnsupportedOperationException}. */
		@Override public void disableAutoInboundFlowControl() {
			throw new UnsupportedOperationException();
		}

		/** Throws {@link UnsupportedOperationException}. */
		@Override public void request(int count) {
			throw new UnsupportedOperationException();
		}

		/** Throws {@link UnsupportedOperationException}. */
		@Override public void setMessageCompression(boolean enable) {
			throw new UnsupportedOperationException();
		}

		static final String OBSERVER_FINALIZED_MESSAGE =
				"onCompleted() has been already called on this substream observer";
	}
}
