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
 * Base class for inbound {@link StreamObserver}s ({@link #ConcurrentInboundObserver(
 * CallStreamObserver, int, BiConsumer, Consumer, ServerCallStreamObserver) request observers} for
 * server RPC method implementations and {@link #ConcurrentInboundObserver(CallStreamObserver, int,
 * BiConsumer, Consumer, Consumer) client response observers} for nested or chained calls) that may
 * dispatch message processing to multiple threads. Handles all the synchronization and manual
 * flow-control to prevent excessive buffering and maintain the number of messages processed
 * concurrently configured with {@code maxConcurrentMessages} constructor param.
 * <p>
 * If work is not dispatched to other threads, then processing of inbound messages will be
 * sequential and performed with respect to flow-control. {@code maxConcurrentMessages} constructor
 * param should usually be set to {@code 1} in such cases.</p>
 * <p>
 * If processing is dispatched to other threads, resulting outbound messages may be sent in
 * different order than the inbound messages arrived: see {@link OrderedConcurrentInboundObserver}
 * if the order needs to be retained.</p>
 * <p>
 * Each inbound message is assigned an individual observer of substream of outbound messages
 * resulting from its processing. Inbound messages together with their individual observers are
 * passed as arguments to {@link #onInboundMessage(Object, CallStreamObserver)} method that must
 * either be overridden in a subclass or its handler must supplied on
 * {@code onInboundMessageHandler} constructor param.<br/>
 * If an application needs to send additional outbound messages not related to any inbound message,
 * it can create additional outbound substream observers using {@link #newOutboundSubstream()}.<br/>
 * Once all outbound substream observers are marked as completed and the inbound stream is closed by
 * the remote peer, {@code onCompleted()} is called automatically on the parent outbound stream
 * observer.<br/>
 * If additional actions need to be taken once the inbound stream is closed by the remote peer,
 * {@link #onHalfClosed()} method can be overridden.</p>
 * <p>
 * If processing of one inbound message may produce multiple outbound messages,
 * {@link #onInboundMessage(Object, CallStreamObserver)} should pass its individual observer to
 * utilities like {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)} in case
 * of sequential processing or
 * {@link DispatchingOnReadyHandler#copyWithFlowControl(CallStreamObserver, Executor, Iterator)} in
 * case work needs to be dispatched to other threads:</p>
 * <pre>
 * class RequestProcessor implements Iterator<ResponseMessage> {
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
 *         (requestMessage, individualRequestMessageResponseObserver) -&gt;
 *                 StreamObservers.copyWithFlowControl(
 *                         new RequestProcessor(requestMessage),
 *                         individualRequestMessageResponseObserver),
 *         (error) -&gt; log.info("error occurred: " + error),
 *         responseObserver
 *     );
 * }</pre>
 *
 * @see OrderedConcurrentInboundObserver
 */
public class ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		implements ClientResponseObserver<ControlT, InboundT> {



	/**
	 * Produces result outbound messages to the given {@code inboundMessage}. Results must be
	 * submitted to {@code individualInboundMessageResultObserver} associated with the given
	 * {@code inboundMessage} using {@link OutboundSubstreamObserver#onNext(Object)
	 * individualInboundMessageResultObserver.onNext(response)}.
	 * Once all responses to the given {@code inboundMessage} are submitted,
	 * {@link OutboundSubstreamObserver#onCompleted()
	 * individualInboundMessageResultObserver.onComplete()} must be called.
	 * <p>
	 * {@code individualInboundMessageResultObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * If processing of one {@code inboundMessage} may produce multiple outbound messages,
	 * this method should respect
	 * {@code individualInboundMessageResultObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() individualInboundMessageResultObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * individualInboundMessageResultObserver.setOnReadyHandler(...)} methods to avoid excessive
	 * buffering. Consider using {@link DispatchingOnReadyHandler} in case work must be dispatched
	 * to other threads or {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}
	 * if not.</p>
	 * <p>
	 * Default implementation calls {@link #onInboundMessageHandler}.</p>
	 *
	 * @see OutboundSubstreamObserver
	 */
	protected void onInboundMessage(
		InboundT inboundMessage,
		CallStreamObserver<OutboundT> individualInboundMessageResultObserver
	) {
		onInboundMessageHandler.accept(inboundMessage, individualInboundMessageResultObserver);
	}

	/**
	 * Called by {@link #onInboundMessage(Object, CallStreamObserver)}.
	 * Initialized via {@code onInboundMessageHandler} param of
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * Consumer, ServerCallStreamObserver)} and
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * Consumer, Consumer)} constructors.
	 */
	protected final BiConsumer<InboundT, CallStreamObserver<OutboundT>> onInboundMessageHandler;



	/**
	 * Default implementation calls {@link #onErrorHandler} if it is not {@code null}.
	 * @see StreamObserver#onError(Throwable)
	 */
	@Override
	public void onError(Throwable error) {
		if (onErrorHandler != null) onErrorHandler.accept(error);
	}

	/**
	 * Called by {@link #onError(Throwable)}. Initialized via {@code onErrorHandler} param of
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * Consumer, ServerCallStreamObserver)} and
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * Consumer, Consumer)} constructors.
	 */
	protected final Consumer<Throwable> onErrorHandler;



	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver)}, default implementation calls
	 * {@link #onBeforeStartHandler} if it is not {@code null}.
	 * This method is meaningful only in case of client response observers, it is never called in
	 * case of server request observers.
	 * @see ClientResponseObserver#beforeStart(ClientCallStreamObserver)
	 */
	protected void onBeforeStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		if (onBeforeStartHandler != null) onBeforeStartHandler.accept(inboundControlObserver);
	}

	/**
	 * Called by {@link #onBeforeStart(ClientCallStreamObserver)}. Initialized via
	 * {@code onPreStartHandler} param of
	 * {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer,
	 * Consumer, Consumer)} constructor.
	 */
	protected final Consumer<ClientCallStreamObserver<ControlT>> onBeforeStartHandler;



	/**
	 * Creates a server request observer: configures flow control, initializes
	 * {@link #onInboundMessageHandler} and {@link #onErrorHandler}.
	 * @param inboundControlObserver server response observer of the given RPC method. Used for
	 *     inbound control: {@link ServerCallStreamObserver#disableAutoInboundFlowControl()
	 *     inboundControlObserver.disableAutoInboundFlowControl()} will be called once at the
	 *     beginning and {@link ServerCallStreamObserver#request(int)
	 *     inboundControlObserver.request(k)} several times throughout the lifetime of the given RPC
	 *     call processing.
	 *     In case of RPC methods that don't issue any nested calls, {@code inboundControlObserver}
	 *     and {@code outboundObserver} will be the same object.
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, Consumer, Consumer) the
	 * other constructor for the descriptions of the remaining params
	 */
	public ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<InboundT, CallStreamObserver<OutboundT>> onInboundMessageHandler,
		Consumer<Throwable> onErrorHandler,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		this(outboundObserver, maxConcurrentMessages, onInboundMessageHandler, onErrorHandler,
				(Consumer<ClientCallStreamObserver<ControlT>>) null);
		setInboundControlObserver(inboundControlObserver);
	}

	/**
	 * Creates a server request observer and configures flow control.
	 * Constructor for those who prefer to override methods rather than provide lambdas as params.
	 * At least {@link #onInboundMessage(Object, CallStreamObserver)} must be overridden.
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, Consumer,
	 * ServerCallStreamObserver) the other constructor for the description of
	 * {@code inboundControlObserver} param
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, Consumer, Consumer) the
	 * other constructor for the descriptions of the remaining params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		this(outboundObserver, maxConcurrentMessages, null, null,
				(Consumer<ClientCallStreamObserver<ControlT>>) null);
		setInboundControlObserver(inboundControlObserver);
	}



	/**
	 * Creates a client response observer:
	 * configures flow control, initializes {@link #onInboundMessageHandler},
	 * {@link #onErrorHandler} and {@link #onBeforeStartHandler}.
	 * @param outboundObserver either the response observer of the given gRPC server method or the
	 *     request observer of a chained client call.
	 * @param maxConcurrentMessages the constructed observer will call
	 *     {@code request(maxConcurrentMessages)} on the {@code inboundControlObserver} (passed via
	 *     the {@link #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, Consumer,
	 *     ServerCallStreamObserver) constructor} param or
	 *     {@link #beforeStart(ClientCallStreamObserver)} param. If message processing is
	 *     dispatched to other threads, this will be the maximum number of inbound messages
	 *     processed concurrently. It should correspond to server's concurrent processing
	 *     capabilities.
	 * @param onInboundMessageHandler stored on {@link #onInboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, CallStreamObserver)}, called by {@link #onNext(Object)}.
	 * @param onErrorHandler stored on {@link #onErrorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param onBeforeStartHandler stored on {@link #onBeforeStartHandler} to be called by
	 *     {@link #onBeforeStart(ClientCallStreamObserver)} called by
	 *     {@link #beforeStart(ClientCallStreamObserver)}. May be empty in case of end-client calls
	 *     not nested in any server calls.
	 */
	public ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<InboundT, CallStreamObserver<OutboundT>> onInboundMessageHandler,
		Consumer<Throwable> onErrorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> onBeforeStartHandler
	) {
		this.outboundObserver = outboundObserver;
		idleCount = maxConcurrentMessages;
		this.onInboundMessageHandler = onInboundMessageHandler;
		this.onErrorHandler = onErrorHandler;
		this.onBeforeStartHandler = onBeforeStartHandler;
		outboundObserver.setOnReadyHandler(this::onReady);
	}

	/**
	 * Creates a client response observer, configures flow control.
	 * Constructor for those who prefer to override methods rather than provide lambdas as params.
	 * At least {@link #onInboundMessage(Object, CallStreamObserver)} must be overridden.
	 * @see #ConcurrentInboundObserver(CallStreamObserver, int, BiConsumer, Consumer, Consumer) the
	 * other constructor for the descriptions of params
	 */
	protected ConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages
	) {
		this(outboundObserver, maxConcurrentMessages, null, null,
			(Consumer<ClientCallStreamObserver<ControlT>>) null);
	}



	final CallStreamObserver<OutboundT> outboundObserver;

	protected CallStreamObserver<ControlT> inboundControlObserver; // for request(n)
	boolean halfClosed = false;
	int idleCount;
	final Set<OutboundSubstreamObserver> activeOutboundSubstreams =
		ConcurrentHashMap.newKeySet();

	protected final Object lock = new Object();



	/**
	 * Called in the constructors in case of the inbound being a server request stream and in
	 * {@link #beforeStart(ClientCallStreamObserver)} in case of the inbound being a response stream
	 * from a previous chained client call.
	 */
	final void setInboundControlObserver(CallStreamObserver<ControlT> inboundControlObserver) {
		if (this.inboundControlObserver != null) {
			throw new IllegalStateException("inboundControlObserver already set");
		}
		this.inboundControlObserver = inboundControlObserver;
		inboundControlObserver.disableAutoInboundFlowControl();
	}



	@Override
	public final void beforeStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		setInboundControlObserver(inboundControlObserver);
		onBeforeStart(inboundControlObserver);
	}



	final void onReady() {
		for (var substreamObserver: activeOutboundSubstreams) {
			// a new request can't arrive now thanks to Listener's concurrency contract
			if (substreamObserver.onReadyHandler != null) substreamObserver.onReadyHandler.run();
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
	 * Calls {@link #onInboundMessage(Object, CallStreamObserver)
	 * onInboundMessage}({@code message}, {@link #newOutboundSubstream()}) and if the parent
	 * outbound observer is ready, then also {@code individualObserver.onReadyHandler.run()}.
	 */
	@Override
	public final void onNext(InboundT message) {
		final var individualObserver = newOutboundSubstream();
		onInboundMessage(message, individualObserver);
		synchronized (lock) {
			if ( !outboundObserver.isReady()) return;
		}
		if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
	}



	/**
	 * Calls {@link #onHalfClosed()}, marks the inbound as completed and if all
	 * {@link #newOutboundSubstream() outbound substreams} are marked as completed, then marks
	 * the parent outbound as completed also.
	 */
	@Override
	public final void onCompleted() {
		onHalfClosed();
		synchronized (lock) {
			halfClosed = true;
			if (activeOutboundSubstreams.isEmpty()) {
				outboundObserver.onCompleted();
			}
		}
	}

	/**
	 * Called at the beginning of {@link #onCompleted()}.
	 */
	protected void onHalfClosed() {}



	/**
	 * Constructs a new {@link OutboundSubstreamObserver OutboundSubstreamObserver}. This method is
	 * called each time a new inbound message arrives in {@link #onNext(Object)}.
	 * <p>
	 * Applications may also create additional outbound substreams to send outbound messages not
	 * related to any inbound message: the parent outbound stream will not be closed until all
	 * such additional substreams are completed.</p>
	 * <p>
	 * Subclasses may override this method if they need to use specialized subclasses of
	 * {@link OutboundSubstreamObserver OutboundSubstreamObserver}: see
	 * {@link OrderedConcurrentInboundObserver#newOutboundSubstream()} for an example.</p>
	 */
	public OutboundSubstreamObserver newOutboundSubstream() {
		return new OutboundSubstreamObserver();
	}



	/**
	 * A thread-safe substream of the parent outbound stream. The parent will not be closed until
	 * all substreams are marked as completed.
	 * @see #newOutboundSubstream()
	 */
	public class OutboundSubstreamObserver extends CallStreamObserver<OutboundT> {

		volatile Runnable onReadyHandler;



		protected OutboundSubstreamObserver() {
			activeOutboundSubstreams.add(this);
		}



		/**
		 * Marks this substream as completed. If all outbound substreams are completed and the
		 * inbound stream is closed, {@code onCompleted()} from the parent outbound observer is
		 * called automatically.
		 */
		@Override
		public void onCompleted() {
			synchronized (lock) {
				if ( !activeOutboundSubstreams.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (halfClosed && activeOutboundSubstreams.isEmpty()) {
					outboundObserver.onCompleted();
					return;
				}

				if (outboundObserver.isReady()) {
					inboundControlObserver.request(1);
				} else {
					idleCount++;
				}
			}
		}



		/**
		 * Forwards {@code message} to the parent outbound observer.
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
		 * Forwards {@code error} to the parent outbound observer.
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
