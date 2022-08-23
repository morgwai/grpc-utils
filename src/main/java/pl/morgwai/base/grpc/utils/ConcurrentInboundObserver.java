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
 * Base class for inbound {@link StreamObserver}s (server method {@link #ConcurrentInboundObserver(
 * CallStreamObserver, int, BiConsumer, BiConsumer, ServerCallStreamObserver) request observers} and
 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer, Consumer)
 * client response observers}), that may dispatch message processing to other threads and that pass
 * results to another {@link CallStreamObserver outboundObserver}.
 * Handles all the synchronization and manual flow-control to prevent excessive buffering while
 * maintaining the number of messages processed concurrently configured with
 * {@code maxConcurrentMessages} constructor param.
 * <p>
 * Each inbound message is assigned an individual observer of outbound messages substream resulting
 * from its processing. Inbound messages together with their individual results observers are
 * passed as arguments to
 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
 * onInboundMessage(message, individualObserver)} method that must either be overridden in a
 * subclass or its handler must supplied on {@code onInboundMessageHandler} constructor param.</p>
 * <p>
 * If an application needs to send additional outbound messages not related to any inbound message,
 * it can create additional outbound substream observers using {@link #newOutboundSubstream()}.<br/>
 * Once all outbound substream observers are marked as completed and the inbound stream is closed by
 * the remote peer, {@link CallStreamObserver#onCompleted() onCompleted()} is called automatically
 * on the parent {@code outboundObserver}.</p>
 * <p>
 * If some additional action is required once the inbound stream is closed by the remote peer,
 * {@link #onHalfClosed()} method may be overridden.</p>
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
 * onInboundMessage(...)} may pass its individual observer to utilities like
 * {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)
 * StreamObservers.copyWithFlowControl(...)} in case of sequential processing or
 * {@link DispatchingOnReadyHandler#copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
 * DispatchingOnReadyHandler.copyWithFlowControl(...)} in case work needs to be dispatched to other
 * threads.</p>
 * @see OrderedConcurrentInboundObserver
 */
public class ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		implements ClientResponseObserver<ControlT, InboundT> {



	/**
	 * Called by {@link #onNext(Object) onNext(inboundMessage)}, processes {@code inboundMessage}.
	 * Resulting outbound messages must be passed to {@code individualInboundMessageResultsObserver}
	 * associated with this given {@code inboundMessage} using
	 * {@link OutboundSubstreamObserver#onNext(Object)
	 * individualInboundMessageResultsObserver.onNext(resultOutboundMessage)}.
	 * Once the processing is done and all the result outbound messages are submitted,
	 * {@link OutboundSubstreamObserver#onCompleted()
	 * individualInboundMessageResultsObserver.onCompleted()} must be called.
	 * <p>
	 * {@code individualInboundMessageResultsObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * If processing of one {@code inboundMessage} may produce multiple outbound messages,
	 * this method should respect
	 * {@code individualInboundMessageResultsObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() individualInboundMessageResultsObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * individualInboundMessageResultsObserver.setOnReadyHandler(...)} methods to avoid excessive
	 * buffering. Consider using {@link DispatchingOnReadyHandler} in case work must be dispatched
	 * to other threads or {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}
	 * otherwise:</p>
	 * <pre>
	 * class RequestProcessor implements Iterator&lt;ResponseMessage&gt; {
	 *     RequestProcessor(RequestMessage request) { ... }
	 *     public boolean hasNext() { ... }
	 *     public Response next() { ... }
	 * }
	 *
	 * public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
	 *         StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
	 *     final var responseObserver =
	 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
	 *     return new ConcurrentInboundObserver&lt;&gt;(
	 *         responseObserver,
	 *         1,
	 *         (requestMessage, individualInboundMessageResultsObserver) -&gt;
	 *                 StreamObservers.copyWithFlowControl(
	 *                         new RequestProcessor(requestMessage),
	 *                         individualRequestMessageResponseObserver),
	 *         (error, thisObserver) -&gt; abortAllRequestProcessors(),
	 *         responseObserver
	 *     );
	 * }</pre>
	 * <p>
	 * The default implementation calls {@link #onInboundMessageHandler}.</p>
	 *
	 * @see OutboundSubstreamObserver
	 */
	protected void onInboundMessage(
		InboundT inboundMessage,
		OutboundSubstreamObserver individualInboundMessageResultsObserver
	) {
		onInboundMessageHandler.accept(inboundMessage, individualInboundMessageResultsObserver);
	}

	/**
	 * Called by the default implementation of
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)}. Initialized via {@code onInboundMessageHandler} param of
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) "server request observer"} and
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, Consumer) "client response observer"} constructors.
	 */
	protected final BiConsumer<InboundT, CallStreamObserver<OutboundT>> onInboundMessageHandler;



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
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver) "server request observer"} and
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, Consumer) "client response observer"} constructors.
	 * The second parameter is a reference to the calling inbound observer, it may be used to call
	 * {@link #reportErrorAfterTasksAndInboundComplete(Throwable)} or {@link #onCompleted()}.
	 */
	protected final BiConsumer<Throwable, ConcurrentInboundObserver<InboundT, OutboundT, ControlT>>
			onErrorHandler;



	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver) beforeStart(inboundControlObserver)},
	 * the default implementation calls {@link #onBeforeStartHandler} if it is not {@code null}.
	 * This method is meaningful only in case of client response observers, it is never called in
	 * case of server request observers.
	 * @see ClientResponseObserver#beforeStart(ClientCallStreamObserver)
	 */
	protected void onBeforeStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		if (onBeforeStartHandler != null) onBeforeStartHandler.accept(inboundControlObserver);
	}

	/**
	 * Called by the default implementation of {@link #onBeforeStart(ClientCallStreamObserver)
	 * beforeStart(inboundControlObserver)}. Initialized via {@code onBeforeStartHandler} param of
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, Consumer) "client response observer"} constructor.
	 */
	protected final Consumer<ClientCallStreamObserver<ControlT>> onBeforeStartHandler;



	/**
	 * Creates a server method request observer: configures its flow-control and initializes
	 * handlers {@link #onInboundMessageHandler} and {@link #onErrorHandler}.
	 * <p>
	 * Example usage in a server method without any nested calls:</p>
	 * <pre>
	 * public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
	 *         StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
	 *     final var responseObserver =
	 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
	 *     return new ConcurrentInboundObserver&lt;&gt;(
	 *         responseObserver,
	 *         MAX_CONCURRENT_REQUESTS,
	 *         (request, individualInboundMessageResultsObserver) -&gt; executor.execute(
	 *             () -&gt; {
	 *                 final var result = process(request);
	 *                 individualInboundMessageResultsObserver.onNext(result);
	 *                 individualInboundMessageResultsObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -&gt; {
	 *             // abort ongoing tasks if needed
	 *         },
	 *         responseObserver
	 *     );
	 * }</pre>
	 * <p>
	 * If a server request observer needs to pass its outbound messages to some nested RPC call
	 * instead of directly back to the client, then it should be created in nested call's
	 * {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)} method and
	 * {@code outboundObserver} constructor argument should be replaced with the nested call's
	 * request observer obtained from
	 * {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver) beforeStart(...)}'s
	 * param.</p>
	 * @param outboundObserver either the response observer of the given server method or the
	 *     request observer of a nested/chained client call.
	 * @param maxConcurrentMessages the constructed observer will call
	 *     {@code request(maxConcurrentMessages)} on the {@code inboundControlObserver} (passed via
	 *     the param below or {@link #beforeStart(ClientCallStreamObserver)} param in case of
	 *     {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 *     Consumer) client response observers}). If message processing is dispatched to other
	 *     threads, this will be the maximum number of inbound messages processed concurrently.
	 *     It should correspond to server's concurrent processing capabilities.
	 * @param onInboundMessageHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 *     onInboundMessageHandler(...)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param inboundControlObserver server response observer of the given RPC method. Used for
	 *     inbound control: {@link ServerCallStreamObserver#disableAutoInboundFlowControl()
	 *     inboundControlObserver.disableAutoInboundFlowControl()} will be called once at the
	 *     beginning and {@link ServerCallStreamObserver#request(int)
	 *     inboundControlObserver.request(k)} several times throughout the lifetime of the given RPC
	 *     call processing.
	 *     In case of RPC methods that don't issue any nested calls, {@code inboundControlObserver}
	 *     and {@code outboundObserver} will be the same object.
	 */
	public ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<? super ControlT> inboundControlObserver
	) {
		this(outboundObserver, maxConcurrentMessages, onInboundMessageHandler, onErrorHandler,
				(Consumer<ClientCallStreamObserver<? super ControlT>>) null);
		setInboundControlObserver(inboundControlObserver);
	}

	/**
	 * Creates a server method request observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide lambdas as params.
	 * At least
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)} must be overridden.
	 * <p>
	 * Example usage in a server method without any nested calls:</p>
	 * <pre>
	 * public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
	 *         StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
	 *     final var responseObserver =
	 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
	 *     return new ConcurrentInboundObserver&lt;&gt;(
	 *         responseObserver,
	 *         MAX_CONCURRENT_REQUESTS,
	 *         responseObserver
	 *     ) {
	 *         &commat;Override protected void onInboundMessage(
	 *             RequestMessage request,
	 *             CallStreamObserver&lt;ResponseMessage&gt; individualInboundMessageResultObserver
	 *         ) {
	 *             executor.execute(
	 *                 () -&gt; {
	 *                     final var result = process(request);
	 *                     individualInboundMessageResultObserver.onNext(result);
	 *                     individualInboundMessageResultObserver.onCompleted();
	 *                 }
	 *             );
	 *         }
	 *
	 *         &commat;Override public void onError(Throwable error) {
	 *             // abort ongoing tasks if needed
	 *         }
	 *     };
	 * }</pre>
	 * <p>
	 * If a server request observer needs to pass its outbound messages to some nested RPC call
	 * instead of directly back to the client, then it should be created in nested call's
	 * {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)} method and
	 * {@code outboundObserver} constructor argument should be replaced with the nested call's
	 * request observer obtained from
	 * {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver) beforeStart(...)}'s
	 * param.</p>
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * ServerCallStreamObserver) the other constructor for the description of the params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		ServerCallStreamObserver<? super ControlT> inboundControlObserver
	) {
		this(outboundObserver, maxConcurrentMessages, null, null,
				(Consumer<ClientCallStreamObserver<? super ControlT>>) null);
		setInboundControlObserver(inboundControlObserver);
	}



	/**
	 * Creates a client response observer,
	 * configures its flow-control and initializes handlers {@link #onInboundMessageHandler},
	 * {@link #onErrorHandler} and {@link #onBeforeStartHandler}.
	 * <p>
	 * Example usage to create a response observer for a nested call forwarding responses to the
	 * parent:</p>
	 * <pre>
	 * public StreamObserver&lt;ParentRequest&gt; parentRPC(
	 *         StreamObserver&lt;ParentResponse&gt; basicResponseObserver) {
	 *     final var parentCallResponseObserver =
	 *             (ServerCallStreamObserver&lt;ParentResponse&gt;) basicResponseObserver;
	 *     final var parentCallRequestObserverHolder = new ParentCallObserverHolder();
	 *     final var nestedCallResponseObserver = new ConcurrentInboundObserver&lt;&gt;(
	 *         parentCallResponseObserver,
	 *         MAX_CONCURRENT_REQUESTS,
	 *         (nestedResponse, individualInboundMessageResultObserver) -&gt; executor.execute(
	 *             () -&gt; {
	 *                 final var result = postProcess(nestedResponse);
	 *                 individualInboundMessageResultObserver.onNext(result);
	 *                 individualInboundMessageResultObserver.onCompleted();
	 *             }
	 *         ),
	 *         (error, thisObserver) -&gt; {
	 *             // abort ongoing tasks if needed
	 *             thisObserver.newOutboundSubstream().onError(error);
	 *         },
	 *         (nestedCallRequestObserver) -&gt; {
	 *             // create parentCallRequestObserver and store it into the holder
	 *         }
	 *     );
	 *     backendStub.nestedRPC(nestedCallResponseObserver);
	 *     return parentCallRequestObserverHolder.get();
	 * }</pre>
	 * @param onBeforeStartHandler stored on {@link #onBeforeStartHandler} to be called by
	 *     {@link #onBeforeStart(ClientCallStreamObserver)} called by
	 *     {@link #beforeStart(ClientCallStreamObserver)}. May be empty in case of end-client calls
	 *     not nested in any server calls.
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer,
	 * ServerCallStreamObserver) the other constructor for the description of the remaining params
	 */
	public ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		Consumer<ClientCallStreamObserver<? super ControlT>> onBeforeStartHandler
	) {
		this.outboundObserver = outboundObserver;
		idleCount = maxConcurrentMessages;
		this.onInboundMessageHandler = onInboundMessageHandler != null
				? onInboundMessageHandler::accept : null;
		this.onErrorHandler = onErrorHandler != null ? onErrorHandler::accept : null;
		this.onBeforeStartHandler = onBeforeStartHandler != null
				? onBeforeStartHandler::accept : null;
		outboundObserver.setOnReadyHandler(this::onReady);
	}

	/**
	 * Creates a client response observer and configures its flow-control.
	 * Constructor for those who prefer to override methods rather than provide lambdas as params.
	 * At least
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)} must be overridden.
	 * <p>
	 * Example usage to create a response observer for a nested call forwarding responses to the
	 * parent:</p>
	 * <pre>
	 * public StreamObserver&lt;ParentRequest&gt; parentRPC(
	 *         StreamObserver&lt;ParentResponse&gt; basicResponseObserver) {
	 *     final var parentCallResponseObserver =
	 *             (ServerCallStreamObserver&lt;ParentResponse&gt;) basicResponseObserver;
	 *     final var parentCallRequestObserverHolder = new ParentCallObserverHolder();
	 *     final var nestedCallResponseObserver = new ConcurrentInboundObserver&lt;&gt;(
	 *         parentCallResponseObserver,
	 *         MAX_CONCURRENT_REQUESTS
	 *     ) {
	 *         &commat;Override protected void onInboundMessage(
	 *             NestedResponse nestedResponse,
	 *             CallStreamObserver&lt;ParentResponse&gt; individualInboundMessageResultObserver
	 *         ) {
	 *             executor.execute(
	 *                 () -&gt; {
	 *                     final var result = postProcess(nestedResponse);
	 *                     individualInboundMessageResultObserver.onNext(result);
	 *                     individualInboundMessageResultObserver.onCompleted();
	 *                 }
	 *             );
	 *         }
	 *
	 *         &commat;Override public void onError(Throwable error) {
	 *             // abort ongoing tasks if needed
	 *             newOutboundSubstream().onError(error);
	 *         }
	 *
	 *         &commat;Override protected void onBeforeStart(
	 *                 ClientCallStreamObserver&lt;NestedRequest&gt; nestedCallRequestObserver) {
	 *             // create parentCallRequestObserver and store it into the holder
	 *         }
	 *     );
	 *     backendStub.nestedRPC(nestedCallResponseObserver);
	 *     return parentCallRequestObserverHolder.get();
	 * }</pre>
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, BiConsumer, Consumer)
	 * the other constructor for the descriptions of params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages
	) {
		this(outboundObserver, maxConcurrentMessages, null, null,
				(Consumer<ClientCallStreamObserver<? super ControlT>>) null);
	}



	final CallStreamObserver<OutboundT> outboundObserver;

	CallStreamObserver<?> inboundControlObserver; // for request(n)
	boolean halfClosed = false;
	int idleCount;  // increased if outbound unready after completing a substream from onNext(...)
	final Set<OutboundSubstreamObserver> activeOutboundSubstreams =
			ConcurrentHashMap.newKeySet();
	Throwable errorToReport;

	final Object lock = new Object();



	/**
	 * Called in the constructors in case of the inbound being a server request stream and in
	 * {@link #beforeStart(ClientCallStreamObserver)} in case of the inbound being a response stream
	 * from a previous chained client call.
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
	 * Calls {@link OutboundSubstreamObserver#onReady() individual onReadyHandlers} of the
	 * {@link #activeOutboundSubstreams active substream observers}, then requests number of
	 * inbound messages equal to {@link #idleCount} from {@link #inboundControlObserver} and resets
	 * the counter to {@code 0}.
	 */
	final void onReady() {
		for (var substreamObserver: activeOutboundSubstreams) {
			// a new request can't arrive now thanks to Listener's concurrency contract
			substreamObserver.onReady();
		}
		synchronized (lock) {
			if (idleCount > 0 && !halfClosed) {
				inboundControlObserver.request(idleCount);
				idleCount = 0;
			}
		}
		// TODO: add routines for checking processing resources availability
	}



	/**
	 * Calls {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage}({@code message}, {@link #newOutboundSubstream()}) and if the parent
	 * {@code outboundObserver} is ready, then also {@code individualObserver.onReadyHandler.run()}.
	 */
	@Override
	public final void onNext(InboundT message) {
		final var individualObserver = newOutboundSubstream();
		individualObserver.requestNextAfterCompletion = true;
		onInboundMessage(message, individualObserver);
		synchronized (lock) {
			if ( !outboundObserver.isReady()) return;
		}
		individualObserver.onReady();
	}



	/**
	 * Indicates that after all tasks and the inbound stream are completed, {@code errorToReport}
	 * should be nevertheless reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(errorToReport)}.
	 * <p>
	 * If after this method is called, any of the remaining individual outbound substream observers
	 * gets a call to its {@link OutboundSubstreamObserver#onError(Throwable) onError(errror)}, then
	 * {@code errorToReport} will be discarded.</p>
	 * <p>
	 * If this method is called from this observer's {@link #onError(Throwable)}, it should be
	 * followed by {@link #onCompleted()} to manually mark inbound stream as completed.<br/>
	 * If it's called in
	 * {@link #onInboundMessage(Object, ConcurrentInboundObserver.OutboundSubstreamObserver)
	 * onInboundMessage(...)}, it should be followed by
	 * {@link OutboundSubstreamObserver#onCompleted() individualObserver.onCompleted()}.</p>
	 */
	public final void reportErrorAfterTasksAndInboundComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
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
	 * Called at the beginning of {@link #onCompleted()}. Subclasses may override this method if
	 * some additional action is required once the inbound stream is closed by the remote peer,
	 */
	protected void onHalfClosed() {}



	/**
	 * Creates a new {@link OutboundSubstreamObserver outbound substream}. This method is
	 * called each time a new inbound message arrives in {@link #onNext(Object)}.
	 * <p>
	 * Applications may also create additional outbound substreams to send outbound messages not
	 * related to any inbound message: the parent {@code outboundObserver} will not be marked as
	 * completed until all substreams are completed.<br/>
	 * After creating an additional substream, it may be necessary to manually deliver the first
	 * call to {@link OutboundSubstreamObserver#onReady() substream observer's onReadyHandler}.</p>
	 * <p>
	 * Subclasses may override this method if they need to use specialized subclasses of
	 * {@link OutboundSubstreamObserver OutboundSubstreamObserver}: see
	 * {@link OrderedConcurrentInboundObserver#newOutboundSubstream()} for an example.</p>
	 */
	public OutboundSubstreamObserver newOutboundSubstream() {
		return new OutboundSubstreamObserver();
	}



	/**
	 * A thread-safe observer of a substream of the parent outbound stream. The parent
	 * {@code outboundObserver} will be marked as completed automatically when and only if all its
	 * substreams are marked as completed.
	 * @see #newOutboundSubstream()
	 */
	public class OutboundSubstreamObserver extends CallStreamObserver<OutboundT> {

		volatile Runnable onReadyHandler;
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



		/**
		 * Forwards {@code message} to the parent {@code outboundObserver}.
		 */
		@Override
		public void onNext(OutboundT message) {
			synchronized (lock) {
				if ( !activeOutboundSubstreams.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				outboundObserver.onNext(message);
			}
		}



		/**
		 * Forwards {@code error} to the parent {@code outboundObserver}.
		 */
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
			if (onReadyHandler != null) onReadyHandler.run();
		}



		/**
		 * Throws {@link UnsupportedOperationException}.
		 */
		@Override public void disableAutoInboundFlowControl() {
			throw new UnsupportedOperationException();
		}

		/**
		 * Throws {@link UnsupportedOperationException}.
		 */
		@Override public void request(int count) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Throws {@link UnsupportedOperationException}.
		 */
		@Override public void setMessageCompression(boolean enable) {
			throw new UnsupportedOperationException();
		}

		static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() has been already called for this observer";
	}
}
