// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.grpc.stub.*;



/**
 * Base class for inbound {@link StreamObserver}s (request observers for server RPC method
 * implementations and client response observers) that may dispatch message processing to multiple
 * threads. Handles all the synchronization and manual flow-control to maintain desired number of
 * messages processed concurrently and prevent excessive buffering.
 * <p>
 * This class should rather not be subclassed directly: see
 * {@link ConcurrentInboundObserverSendingToParent} and
 * {@link ConcurrentInboundObserverSendingToNested}.</p>
 * <p>
 * If processing is not dispatched to other threads, then processing of inbound messages will be
 * sequential with respect to flow-control. {@code numberOfInitialMessages} constructor param should
 * usually be set to 1 in such cases.</p>
 * <p>
 * If processing is dispatched to other threads, results may be sent in different order: see
 * {@link OrderedConcurrentInboundObserver} if the order needs to be retained.</p>
 * <p>
 * Typical server usage:</p>
 * <pre>
 * public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
 *         StreamObserver&lt;ResponseMessage&gt; responseObserver) {
 *     return new ConcurrentRequestObserver&lt;&gt;(
 *         (ServerCallStreamObserver&lt;ResponseMessage&gt;) responseObserver,
 *         MAX_CONCURRENT_REQUESTS,
 *         (requestMessage, individualRequestMessageResponseObserver) -&gt; executor.execute(
 *             () -&gt; {
 *                 final var responseMessage = process(requestMessage);
 *                 individualRequestMessageResponseObserver.onNext(responseMessage);
 *                 individualRequestMessageResponseObserver.onCompleted();
 *             }),
 *         (error) -&gt; log.info(error)
 *     );
 * }</pre>
 * <p>
 * Once individual observers of results for all inbound messages are closed and the inbound stream
 * is closed by the remote peer, {@code onCompleted()} is called automatically on the outbound
 * stream observer. If an application needs to send additional outbound messages not related to any
 * inbound message, it can create additional individual observers using
 * {@link #newIndividualObserver()}.</p>
 * <p>
 * If one request message may produce multiple responses, this class can be combined with utilities
 * like {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)} in case of
 * sequential processing or {@link DispatchingOnReadyHandler} in case of concurrent processing:</p>
 * <pre>
 * class RequestProcessor implements Iterator<ResponseMessage> {
 *     RequestProcessor(RequestMessage request) { ... }
 *     public boolean hasNext() { ... }
 *     public Response next() { ... }
 * }
 *
 * public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
 *         StreamObserver&lt;ResponseMessage&gt; responseObserver) {
 *     return new ConcurrentRequestObserver&lt;&gt;(
 *         (ServerCallStreamObserver&lt;ResponseMessage&gt;) responseObserver,
 *         MAX_CONCURRENT_REQUESTS,
 *         (requestMessage, individualRequestMessageResponseObserver) -&gt;
 *                 StreamObservers.copyWithFlowControl(
 *                         new RequestProcessor(requestMessage),
 *                         individualRequestMessageResponseObserver),
 *         (error) -&gt; log.info("error occurred: " + error)
 *     );
 * }</pre>
 * @see ConcurrentInboundObserverSendingToParent
 * @see ConcurrentInboundObserverSendingToNested
 * @see OrderedConcurrentInboundObserver
 * @see OrderedConcurrentInboundObserverSendingToParent
 * @see OrderedConcurrentInboundObserverSendingToChained
 */
public abstract class ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		implements ClientResponseObserver<ControlT, InboundT> {



	/**
	 * Default implementation calls {@link #errorHandler}.
	 * @see StreamObserver#onError(Throwable)
	 */
	@Override
	public void onError(Throwable error) {
		if (errorHandler != null) errorHandler.accept(error);
	}

	/**
	 * Called by {@link #onError(Throwable)}. Initialized via the param of
	 * {@link #ConcurrentInboundObserver(ClientCallStreamObserver, int, Consumer, Consumer)} and
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, Consumer, Consumer)}
	 * constructors.
	 */
	protected final Consumer<Throwable> errorHandler;



	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver)}, default implementation calls
	 * {@link #preStartHandler}.
	 * @see ClientResponseObserver#beforeStart(ClientCallStreamObserver)
	 */
	protected void onPreStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		if (preStartHandler != null) preStartHandler.accept(inboundControlObserver);
	}

	/**
	 * Called by {@link #onPreStart(ClientCallStreamObserver)}. Initialized via the param of
	 * {@link #ConcurrentInboundObserver(ClientCallStreamObserver, int, Consumer, Consumer)} and
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, Consumer, Consumer)}
	 * constructors.
	 */
	protected final Consumer<ClientCallStreamObserver<ControlT>> preStartHandler;



	/**
	 * Configures flow control, initializes {@link #errorHandler} and {@link #preStartHandler}.
	 * @param serverResponseObserver response observer of the given gRPC method.
	 * @param numberOfInitialMessages the constructed observer will call
	 *     {@code request(numberOfInitiallyRequestedMessages)} on observer set by
	 *     {@link #setInboundControlObserver(CallStreamObserver)}. If message processing is
	 *     dispatched to other threads, this will be the maximum number of inbound messages
	 *     processed concurrently. It should correspond to server's concurrent processing
	 *     capabilities.
	 * @param errorHandler stored on {@link #errorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 * @param preStartHandler stored on {@link #preStartHandler} to be called by
	 *     {@link #onPreStart(ClientCallStreamObserver)}
	 */
	protected ConcurrentInboundObserver(
		ServerCallStreamObserver<OutboundT> serverResponseObserver,
		int numberOfInitialMessages,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		this(null, serverResponseObserver, numberOfInitialMessages, errorHandler, preStartHandler);
	}

	/**
	 * Version of
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, Consumer, Consumer)} for
	 * cases when the outbound is a {@link ClientCallStreamObserver client request observer}.
	 */
	protected ConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		int numberOfInitialMessages,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		this(clientRequestObserver, clientRequestObserver, numberOfInitialMessages, errorHandler,
				preStartHandler);
	}



	private ConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		CallStreamObserver<OutboundT> outboundObserver,
		int numberOfInitialMessages,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		this.clientRequestObserver = clientRequestObserver;
		this.outboundObserver = outboundObserver;
		idleCount = numberOfInitialMessages;
		this.errorHandler = errorHandler;
		this.preStartHandler = preStartHandler;
		outboundObserver.setOnReadyHandler(this::onOutboundReady);
	}



	final CallStreamObserver<OutboundT> outboundObserver;
	final ClientCallStreamObserver<OutboundT> clientRequestObserver;// copy of above for cancelling

	protected CallStreamObserver<?> inboundControlObserver; // for request(n)
	boolean inboundClosed = false;
	int idleCount;
	final Set<IndividualInboundMessageResultObserver> activeIndividualObservers =
		ConcurrentHashMap.newKeySet();

	protected final Object lock = new Object();



	/**
	 * Called in the constructors in case of inbound being server request messages and in
	 * {@link #beforeStart(ClientCallStreamObserver)} in case of inbound being response messages
	 * from a previous chained client call.
	 */
	protected final void setInboundControlObserver(CallStreamObserver<?> inboundControlObserver) {
		if (this.inboundControlObserver != null) {
			throw new IllegalStateException("inboundControlObserver already set");
		}
		this.inboundControlObserver = inboundControlObserver;
		inboundControlObserver.disableAutoInboundFlowControl();
	}



	@Override
	public final void beforeStart(ClientCallStreamObserver<ControlT> inboundControlObserver) {
		setInboundControlObserver(inboundControlObserver);
		onPreStart(inboundControlObserver);
	}



	void onOutboundReady() {
		synchronized (lock) {
			if (idleCount > 0 && !inboundClosed) {
				inboundControlObserver.request(idleCount);
				idleCount = 0;
			}
		}
		for (var individualObserver: activeIndividualObservers) {
			// a new request can't arrive now thanks to Listener's concurrency contract
			if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
		}
		// TODO: add routines for checking processing resources availability
	}



	@Override
	public void onCompleted() {
		synchronized (lock) {
			inboundClosed = true;
			if (activeIndividualObservers.isEmpty()) {
				outboundObserver.onCompleted();
			}
		}
	}



	/**
	 * Constructs a new
	 * {@link IndividualInboundMessageResultObserver IndividualInboundMessageResultObserver}. This
	 * method is called automatically each time a new inbound message arrives in
	 * {@link #onNext(Object)}. Applications may also create additional observers to send outbound
	 * messages not related to any inbound message: outbound stream will not be closed until all
	 * such additional observers are closed.
	 * <p>
	 * Subclasses may override this method if they need to use specialized subclasses of
	 * {@link IndividualInboundMessageResultObserver IndividualInboundMessageResultObserver}: see
	 * {@link OrderedConcurrentInboundObserver#newIndividualObserver()} for example.</p>
	 */
	public IndividualInboundMessageResultObserver newIndividualObserver() {
		return new IndividualInboundMessageResultObserver();
	}



	/**
	 * Observer of results of processing of 1 particular inbound message. All methods are
	 * thread-safe. A new instance is created each time a new inbound message arrives via
	 * {@link #onNext(Object)}.
	 */
	protected class IndividualInboundMessageResultObserver
			extends ClientCallStreamObserver<OutboundT> {

		volatile Runnable onReadyHandler;



		protected IndividualInboundMessageResultObserver() {
			activeIndividualObservers.add(this);
		}



		/**
		 * Indicates that processing of the associated inbound message is completed. Once all
		 * individual observers are completed and the inbound stream is closed,
		 * {@code onCompleted()} from the parent outbound observer is called automatically.
		 */
		@Override
		public void onCompleted() {
			synchronized (lock) {
				if ( !activeIndividualObservers.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (inboundClosed && activeIndividualObservers.isEmpty()) {
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
				if ( !activeIndividualObservers.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				outboundObserver.onNext(message);
			}
		}



		/**
		 * If the parent outbound observer is a {@link ServerCallStreamObserver} then calls
		 * {@code onError(error)} from the parent. If the parent is a
		 * {@link ClientCallStreamObserver} then calls {@code cancel(error.getMessage(), error)}.
		 */
		@Override
		public void onError(Throwable error) {
			synchronized (lock) {
				if ( !activeIndividualObservers.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				outboundObserver.onError(error);
			}
		}



		@Override
		public void cancel(@Nullable String message, @Nullable Throwable cause) {
			synchronized (lock) {
				if ( !activeIndividualObservers.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				clientRequestObserver.cancel(message, cause);
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
	}



	static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() or onError() has been already called for this observer";
}
