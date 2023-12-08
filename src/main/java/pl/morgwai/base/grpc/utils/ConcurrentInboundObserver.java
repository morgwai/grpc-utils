// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
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
 * {@link #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
 * ServerCallStreamObserver) request observers} and {@link #newConcurrentClientResponseObserver(
 * CallStreamObserver, int, BiConsumer, BiConsumer, Consumer) client response observers}), that pass
 * results to some {@link CallStreamObserver outboundObserver} and may dispatch message processing
 * to other threads.
 * Handles all the synchronization and manual flow-control to maintain the number of messages
 * processed concurrently configured with {@code maxConcurrentMessages} constructor param.
 * <p>
 * Each inbound message is assigned a separate dedicated {@link OutboundSubstreamObserver observer
 * of the substream of outbound messages} resulting from processing of this inbound message. Inbound
 * messages together with their dedicated results observers are passed as arguments to
 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
 * onInboundMessage(message, messageResultsDedicatedObserver)} method that must either be overridden
 * in a subclass or its handler must supplied on {@code onInboundMessageHandler} constructor param.
 * </p>
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
 * sequential and performed with respect to flow-control. {@code maxConcurrentMessages} constructor
 * param should usually be set to {@code 1} in such cases.</p>
 * <p>
 * If work is dispatched to other threads, resulting outbound messages may be sent in a different
 * order than the inbound messages arrived: see {@link OrderedConcurrentInboundObserver} if the
 * order needs to be preserved.</p>
 * <p>
 * If processing of one inbound message may produce multiple outbound messages,
 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
 * onInboundMessage(...)} may pass its dedicated observer to utilities like
 * {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)
 * StreamObservers.copyWithFlowControl(...)} in case of sequential processing or
 * {@link DispatchingOnReadyHandler#copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
 * DispatchingOnReadyHandler.copyWithFlowControl(...)} in case work needs to be dispatched to other
 * threads.</p>
 * <p>
 * Instances can be created either using "named constructor" static methods
 * ({@link #newSimpleConcurrentServerRequestObserver(ServerCallStreamObserver, int, BiConsumer,
 * BiConsumer) newSimpleConcurrentServerRequestObserver(...)},
 * {@link #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
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



	/** See {@link ConcurrentInboundObserver.OutboundSubstreamObserver} */
	public static abstract class SubstreamObserver<MessageT> extends CallStreamObserver<MessageT> {

		/** See {@link ConcurrentInboundObserver#reportErrorAfterAllTasksComplete(Throwable)} */
		public abstract void reportErrorAfterAllTasksComplete(Throwable errorToReport);

		/** See {@link ConcurrentInboundObserver#newOutboundSubstream()} */
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
	 *         (request, requestResultsDedicatedObserver) -> executor.execute(
	 *             () -> {
	 *                 final var response = process(request);
	 *                 requestResultsDedicatedObserver.onNext(response);
	 *                 requestResultsDedicatedObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -> {}
	 *     );
	 * }}</pre>
	 *
	 * @param responseObserver response observer of the given server method.
	 * @param maxConcurrentMessages the constructed observer will call
	 *     {@link CallStreamObserver#request(int) responseObserver.request(maxConcurrentMessages)}.
	 *     If message processing is dispatched to other threads, this will in consequence be the
	 *     maximum number of inbound messages processed concurrently. It should correspond to
	 *     server's concurrent processing capabilities.
	 * @param onRequestMessageHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 *
	 * @see #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     ServerCallStreamObserver) newConcurrentServerRequestObserver(...) in case the server
	 *     method needs to issue some nested client calls.
	 */
	public static <RequestT, ResponseT>
	ConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
	newSimpleConcurrentServerRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int maxConcurrentMessages,
		BiConsumer<? super RequestT, ? super SubstreamObserver<ResponseT>> onRequestMessageHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
				> onErrorHandler
	) {
		return newConcurrentServerRequestObserver(
			responseObserver,
			maxConcurrentMessages,
			onRequestMessageHandler,
			onErrorHandler,
			responseObserver
		);
	}



	/**
	 * Creates a server method request observer for gRPC methods that may issue some nested client
	 * calls.
	 * Configures flow-control and initializes handlers {@link #onInboundMessageHandler} and
	 * {@link #onErrorHandler}.
	 * <p>
	 * Example:</p>
	 * <pre>{@code
	 * public StreamObserver<OuterRequest> outerRpc(StreamObserver<OuterResponse> responseObserver)
	 * {
	 *     final var outerResponseObserver =
	 *             (ServerCallStreamObserver<OuterResponse>) responseObserver;
	 *     outerResponseObserver.setOnCancelHandler(() -> {});
	 *     final StreamObserver<OuterRequest>[] outerRequestObserverHolder = {null};
	 *     backendStub.nestedRpc(newConcurrentClientResponseObserver(
	 *         outerResponseObserver,
	 *         MAX_CONCURRENTLY_PROCESSED_NESTED_RESPONSES,
	 *         (nestedResponse, nestedResponseResultsDedicatedObserver) ->  executor.execute(
	 *             () -> {
	 *                 final var outerResponse = postProcess(nestedResponse);
	 *                 nestedResponseResultsDedicatedObserver.onNext(outerResponse);
	 *                 nestedResponseResultsDedicatedObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -> {
	 *             thisObserver.reportErrorAfterTasksAndInboundComplete(error);
	 *             thisObserver.onCompleted();
	 *         },
	 *         (nestedRequestObserver) -> {
	 *             outerRequestObserverHolder[0] = newConcurrentServerRequestObserver(
	 *                 nestedRequestObserver,
	 *                 MAX_CONCURRENTLY_PROCESSED_OUTER_REQUESTS,
	 *                 (outerRequest, outerRequestResultsDedicatedObserver) -> executor.execute(
	 *                     () -> {
	 *                         final var nestedRequest = preProcess(outerRequest);
	 *                         outerRequestResultsDedicatedObserver.onNext(nestedRequest);
	 *                         outerRequestResultsDedicatedObserver.onCompleted();
	 *                     }
	 *                 ),
	 *                 (error, thisObserver) -> {
	 *                     // client cancelled, abort tasks if needed
	 *                 },
	 *                 outerResponseObserver
	 *             );
	 *         }
	 *     ));
	 *     return outerRequestObserverHolder[0];
	 * }}</pre>
	 *
	 * @param nestedRequestObserver request observer of the nested client call to which results
	 *     should be streamed
	 * @param maxConcurrentMessages the constructed observer will call
	 *     {@link CallStreamObserver#request(int)
	 *     outerResponseObserver.request(maxConcurrentMessages)}.
	 *     If message processing is dispatched to other threads, this will in consequence be the
	 *     maximum number of inbound messages processed concurrently. It should correspond to
	 *     server's concurrent processing capabilities.
	 * @param onOuterRequestMessageHandler stored on {@link #onInboundMessageHandler} to be called
	 *     by
	 *     {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param outerResponseObserver server response observer of the given RPC method used for
	 *     inbound control: {@link ServerCallStreamObserver#disableAutoInboundFlowControl()
	 *     outerResponseObserver.disableAutoInboundFlowControl()} will be called once at the
	 *     beginning and {@link ServerCallStreamObserver#request(int)
	 *     outerResponseObserver.request(k)} several times throughout the lifetime of the given RPC
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
		CallStreamObserver<NestedRequestT> nestedRequestObserver,
		int maxConcurrentMessages,
		BiConsumer<? super ServerRequestT, ? super SubstreamObserver<NestedRequestT>>
				onOuterRequestMessageHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<
							ServerRequestT, NestedRequestT, ServerResponseT>
				> onErrorHandler,
		ServerCallStreamObserver<ServerResponseT> outerResponseObserver
	) {
		return new ConcurrentInboundObserver<>(
			nestedRequestObserver,
			maxConcurrentMessages,
			onOuterRequestMessageHandler,
			onErrorHandler,
			outerResponseObserver
		);
	}



	/**
	 * Creates a client call response observer.
	 * Configures flow-control and initializes handlers {@link #onInboundMessageHandler},
	 * {@link #onErrorHandler} and {@link #onBeforeStartHandler}.
	 * The created observer may either be forwarding results to the request observer of another
	 * "chained" client call, or to the response observer of its outer server call, if the client
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
	 *             (headCallResponse, headCallResponseResultsDedicatedObserver) -> executor.execute(
	 *                 () -> {
	 *                     final var chainedRequest = midProcess(headCallResponse);
	 *                     headCallResponseResultsDedicatedObserver.onNext(chainedRequest);
	 *                     headCallResponseResultsDedicatedObserver.onCompleted();
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
	 * See {@link #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) newConcurrentServerRequestObserver(...)} for a nested
	 * call example.</p>
	 *
	 * @param outboundObserver either the request observer of another "chained" client call or the
	 *     response observer of the outer server call that the observer being created is nested in.
	 * @param maxConcurrentMessages the constructed observer will call
	 *     {@link CallStreamObserver#request(int)} on the call's request observer obtained via
	 *     {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)}.
	 *     If message processing is dispatched to other threads, this will in consequence be the
	 *     maximum number of inbound messages processed concurrently. It should correspond to
	 *     server's concurrent processing capabilities.
	 * @param onClientResponseHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)}.
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
		int maxConcurrentMessages,
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
			maxConcurrentMessages,
			onClientResponseHandler,
			onErrorHandler,
			onBeforeStartHandler
		);
	}



	/**
	 * Creates a server method request observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. At least
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)} must be overridden.
	 * @see #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     ServerCallStreamObserver) newConcurrentServerRequestObserver(...) for descriptions of the
	 *     params.
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		ServerCallStreamObserver<? super ControlT> inboundControlObserver
	) {
		this(outboundObserver, maxConcurrentMessages, null, null,
			(Consumer<ClientCallStreamObserver<ControlT>>) null);
		setInboundControlObserver(inboundControlObserver);
	}

	/**
	 * Creates a client response observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. At least
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)} must be overridden.
	 * @see #newConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     Consumer) newConcurrentClientResponseObserver(...) for descriptions of the params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages
	) {
		this(
			outboundObserver,
			maxConcurrentMessages,
			null,
			null,
			(Consumer<? super ClientCallStreamObserver<ControlT>>) null
		);
	}



	/**
	 * Called by {@link #onNext(Object) onNext(message)}, processes {@code message}.
	 * Resulting outbound messages must be passed to {@code messageResultsDedicatedObserver}
	 * associated with this given {@code message} using
	 * {@link OutboundSubstreamObserver#onNext(Object)
	 * messageResultsDedicatedObserver.onNext(resultOutboundMessage)}. Once the processing is done
	 * and all the result outbound messages are submitted, either
	 * {@link OutboundSubstreamObserver#onCompleted() messageResultsDedicatedObserver.onCompleted()}
	 * or {@link OutboundSubstreamObserver#onError(Throwable)
	 * messageResultsDedicatedObserver.onError(...)} must be called.
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
	 * class RequestProcessor implements Iterator<ResponseMessage> {
	 *     RequestProcessor(RequestMessage request) { ... }
	 *     public boolean hasNext() { ... }
	 *     public Response next() { ... }
	 * }
	 *
	 * public StreamObserver<RequestMessage> myBiDiMethod(
	 *         StreamObserver<ResponseMessage> basicResponseObserver) {
	 *     final var responseObserver =
	 *             (ServerCallStreamObserver<ResponseMessage>) basicResponseObserver;
	 *     return newSimpleConcurrentServerRequestObserver(
	 *         responseObserver,
	 *         1,
	 *         (message, messageResultsDedicatedObserver) -> StreamObservers.copyWithFlowControl(
	 *             new RequestProcessor(message),
	 *             messageResultsDedicatedObserver
	 *         ),
	 *         (error, thisObserver) -> responseObserver.onError(error);
	 *     );
	 * }}</pre>
	 * <p>
	 * The default implementation calls {@link #onInboundMessageHandler}.</p>
	 *
	 * @see OutboundSubstreamObserver
	 */
	protected void onInboundMessage(
		InboundT message,
		OutboundSubstreamObserver messageResultsDedicatedObserver
	) {
		onInboundMessageHandler.accept(message, messageResultsDedicatedObserver);
	}

	/**
	 * Called by the default implementation of
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)}. Initialized via {@code onInboundMessageHandler} param of
	 * {@link #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * ServerCallStreamObserver) newConcurrentServerRequestObserver(...)},
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
	 * {@link #newConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * ServerCallStreamObserver) newConcurrentServerRequestObserver(...)},
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
	 * to its {@link OutboundSubstreamObserver#onError(Throwable) onError(errror)}, then
	 * {@code errorToReport} will be discarded.</p>
	 * <p>
	 * If this method is called from this observer's {@link #onError(Throwable)}, it should be
	 * followed by {@link #onCompleted()} to manually mark inbound stream as completed
	 * (half-close).<br/>
	 * If it's called in
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)}, it should be eventually followed by
	 * {@link OutboundSubstreamObserver#onCompleted() dedicatedObserver.onCompleted()}.</p>
	 */
	public final void reportErrorAfterAllTasksComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
	}



	/**
	 * Creates a new {@link OutboundSubstreamObserver outbound substream}. This method is
	 * called each time a new inbound message arrives in {@link #onNext(Object)}.
	 * <p>
	 * Applications may also create additional outbound substreams to send outbound messages not
	 * related to any inbound message: the parent {@code outboundObserver} will not be marked as
	 * completed until all substreams are completed.<br/>
	 * After creating an additional substream and
	 * {@link OutboundSubstreamObserver#setOnReadyHandler(Runnable) setting its onReadyHandler}, it
	 * may be necessary to manually deliver the first {@link OutboundSubstreamObserver#onReady()
	 * onReady() call}.</p>
	 * <p>
	 * Subclasses may override this method if they need to use specialized subclasses of
	 * {@link OutboundSubstreamObserver OutboundSubstreamObserver}: see
	 * {@link OrderedConcurrentInboundObserver#newOutboundSubstream()} for an example.</p>
	 */
	public OutboundSubstreamObserver newOutboundSubstream() {
		return new OutboundSubstreamObserver();
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



	/** Low-level constructor for server observers. */
	ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		this(
			outboundObserver,
			maxConcurrentMessages,
			onInboundMessageHandler,
			onErrorHandler,
			(Consumer<? super ClientCallStreamObserver<ControlT>>) null
		);
		setInboundControlObserver(inboundControlObserver);
	}



	/** Lowest level constructor that does the actual initialization. */
	ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		Consumer<? super ClientCallStreamObserver<ControlT>> onBeforeStartHandler
	) {
		this.outboundObserver = outboundObserver;
		idleCount = maxConcurrentMessages;
		this.onInboundMessageHandler = onInboundMessageHandler;
		this.onErrorHandler = onErrorHandler;
		this.onBeforeStartHandler = onBeforeStartHandler;
		outboundObserver.setOnReadyHandler(this::onReady);
	}



	/**
	 * Called in constructors in case of server method request observers and in
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
	 * Calls {@link OutboundSubstreamObserver#onReady() dedicated onReadyHandlers} of the
	 * {@link #activeOutboundSubstreams active substream observers}, then requests number of
	 * inbound messages equal to {@link #idleCount} from {@link #inboundControlObserver} and resets
	 * the counter to {@code 0}.
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
	 * Calls {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage}({@code message}, {@link #newOutboundSubstream()}) and if the parent
	 * {@code outboundObserver} is ready, then also {@link OutboundSubstreamObserver#onReady()
	 * messageResultsDedicatedObserver.onReadyHandler}.
	 */
	@Override
	public final void onNext(InboundT message) {
		final var messageResultsDedicatedObserver = newOutboundSubstream();
		messageResultsDedicatedObserver.requestNextAfterCompletion = true;
		onInboundMessage(message, messageResultsDedicatedObserver);
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
	 * A thread-safe observer of a substream of the parent outbound stream. The parent
	 * {@code outboundObserver} will be marked as completed automatically when and only if all its
	 * substreams and the inbound stream are marked as completed.
	 * @see #newOutboundSubstream()
	 */
	public class OutboundSubstreamObserver extends SubstreamObserver<OutboundT> {

		// Listener's / parent observer's concurrency contract makes it unnecessary to synchronize
		// setting or calling onReadyHandler, but calling and setting may still be performed by
		// different threads (just not concurrently), hence volatile.
		volatile Runnable onReadyHandler;

		/**
		 * Set to {@code true} at the beginning of {@link ConcurrentInboundObserver#onNext(Object)},
		 * prevents requesting a next inbound message on completion of additional user created
		 * substreams.
		 */
		boolean requestNextAfterCompletion = false;



		protected OutboundSubstreamObserver() {
			activeOutboundSubstreams.add(this);
		}



		/**
		 * Marks this substream as completed. If all outbound substreams are completed and the
		 * inbound stream is closed, {@link CallStreamObserver#onCompleted() onCompleted()} from the
		 * parent {@code outboundObserver} is called automatically.
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



		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			this.onReadyHandler = onReadyHandler;
		}



		/**
		 * Calls the handler registered via {@link #setOnReadyHandler(Runnable)} if it's not
		 * {@code null}.
		 */
		public void onReady() {
			// TODO: maybe lock onReadyHandler
			if (onReadyHandler != null) onReadyHandler.run();
		}



		/** Calls {@link ConcurrentInboundObserver#reportErrorAfterAllTasksComplete(Throwable)} */
		@Override
		public final void reportErrorAfterAllTasksComplete(Throwable errorToReport) {
			ConcurrentInboundObserver.this.reportErrorAfterAllTasksComplete(errorToReport);
		}



		/** Calls {@link ConcurrentInboundObserver#newOutboundSubstream()} */
		@Override
		public SubstreamObserver<OutboundT> newOutboundSubstream() {
			return ConcurrentInboundObserver.this.newOutboundSubstream();
		}



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
