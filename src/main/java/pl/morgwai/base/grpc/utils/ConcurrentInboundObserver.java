// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.grpc.stub.*;



/**
 * Base class for inbound {@link StreamObserver}s (request observers for server RPC method
 * implementations and client response observers) that may dispatch message processing to multiple
 * threads. Handles all the synchronization and manual flow-control to maintain desired number of
 * messages processed concurrently and prevent excessive buffering.
 * <p>
 * This class should rather not be subclassed directly: see {@link ConcurrentRequestObserver} for
 * server request observers and {@link ConcurrentResponseObserver} for client response
 * observers.</p>
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
 * @see ConcurrentRequestObserver
 * @see ConcurrentResponseObserver
 * @see OrderedConcurrentInboundObserver
 * @see OrderedConcurrentRequestObserver
 * @see OrderedConcurrentResponseObserver
 */
public abstract class ConcurrentInboundObserver<InboundT, OutboundT>
		implements StreamObserver<InboundT> {



	/**
	 * Produces response messages to the given {@code inboundMessage}. Responses must be submitted
	 * to {@code individualObserver} that is associated with this
	 * {@code inboundMessage} using {@link CallStreamObserver#onNext(Object)
	 * individualObserver.onNext(response)}.
	 * Once all responses to this {@code inboundMessage} are submitted, this method must call
	 * {@link IndividualInboundMessageResultObserver#onCompleted()
	 * individualObserver.onComplete()}.
	 * <p>
	 * {@code individualObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * {@code individualObserver.cancel(...)} can only be called if the parent
	 * outbound observer is a {@link ClientCallStreamObserver}.</p>
	 * <p>
	 * To avoid excessive buffering, implementations should respect
	 * {@code individualObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() individualObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * individualObserver.setOnReadyHandler(...)} methods.<br/>
	 * Consider using {@link DispatchingOnReadyHandler} or
	 * {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}.</p>
	 * <p>
	 * Default implementation calls {@link #inboundMessageHandler}.</p>
	 *
	 * @see IndividualInboundMessageResultObserver
	 */
	protected void onInboundMessage(
		InboundT inboundMessage,
		IndividualInboundMessageResultObserver individualObserver
	) {
		inboundMessageHandler.accept(inboundMessage, individualObserver);
	}

	/**
	 * Called by {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)}.
	 * Initialized via the param of
	 * {@link #ConcurrentInboundObserver(ClientCallStreamObserver, int, BiConsumer, Consumer)} and
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructors.
	 */
	protected BiConsumer<InboundT, ClientCallStreamObserver<OutboundT>> inboundMessageHandler;



	/**
	 * Default implementation calls {@link #errorHandler}.
	 * @see StreamObserver#onError(Throwable)
	 */
	@Override
	public void onError(Throwable t) {
		errorHandler.accept(t);
	}

	/**
	 * Called by {@link #onError(Throwable)}. Initialized via the param of
	 * {@link #ConcurrentInboundObserver(ClientCallStreamObserver, int, BiConsumer, Consumer)} and
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructors.
	 */
	protected Consumer<Throwable> errorHandler;



	/**
	 * Initializes {@link #inboundMessageHandler} and {@link #errorHandler} and calls
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int)}.
	 *
	 * @param inboundMessageHandler stored on {@link #inboundMessageHandler} to be called by
	 *     {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)}.
	 * @param errorHandler stored on {@link #errorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 */
	public ConcurrentInboundObserver(
		ServerCallStreamObserver<OutboundT> serverResponseObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, CallStreamObserver<OutboundT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler
	) {
		this(serverResponseObserver, numberOfInitialMessages);
		this.inboundMessageHandler = inboundMessageHandler::accept;
		this.errorHandler = errorHandler;
	}

	/**
	 * Version of
	 * {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)} for
	 * cases when the outbound is a {@link ClientCallStreamObserver client request observer}.
	 */
	public ConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, ClientCallStreamObserver<OutboundT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler
	) {
		this(clientRequestObserver, numberOfInitialMessages);
		this.inboundMessageHandler = inboundMessageHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * Configures flow control.
	 * Constructor for those who prefer to override
	 * {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)} and
	 * {@link #onError(Throwable)} in a subclass instead of providing lambdas.
	 *
	 * @param serverResponseObserver response observer of the given gRPC method.
	 * @param numberOfInitialMessages the constructed observer will call
	 *     {@code responseObserver.request(numberOfInitiallyRequestedMessages)}. If
	 *     {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)} dispatches work
	 *     to other threads, rather than processing synchronously, this will be the maximum number
	 *     of request messages processed concurrently. It should correspond to server's concurrent
	 *     processing capabilities.
	 */
	protected ConcurrentInboundObserver(
		ServerCallStreamObserver<OutboundT> serverResponseObserver,
		int numberOfInitialMessages
	) {
		this(null, serverResponseObserver, numberOfInitialMessages);
	}

	/**
	 * Version of {@link #ConcurrentInboundObserver(ServerCallStreamObserver, int)} for cases when
	 * the outbound is a {@link ClientCallStreamObserver client request observer}.
	 */
	protected ConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		int numberOfInitialMessages
	) {
		this(clientRequestObserver, clientRequestObserver, numberOfInitialMessages);
	}

	private ConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		CallStreamObserver<OutboundT> outboundObserver,
		int numberOfInitialMessages
	) {
		this.clientRequestObserver = clientRequestObserver;
		this.outboundObserver = outboundObserver;
		idleCount = numberOfInitialMessages;
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
	 * Called in the constructors in case of server {@link ConcurrentRequestObserver} and in case of
	 * client {@link ConcurrentResponseObserver} in
	 * {@link ConcurrentResponseObserver#beforeStart(ClientCallStreamObserver) beforeStart(...)}.
	 */
	protected final void setInboundControlObserver(CallStreamObserver<?> inboundControlObserver) {
		if (this.inboundControlObserver != null) {
			throw new IllegalStateException("inboundControlObserver already set");
		}
		this.inboundControlObserver = inboundControlObserver;
		inboundControlObserver.disableAutoInboundFlowControl();
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
	 * Calls {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)
	 * onRequest}({@code request}, {@link #newIndividualObserver()}) and if the outbound observer is
	 * ready, then also {@code individualObserver.onReadyHandler.run()}.
	 */
	@Override
	public final void onNext(InboundT request) {
		final var individualObserver = newIndividualObserver();
		onInboundMessage(request, individualObserver);
		synchronized (lock) {
			if ( !outboundObserver.isReady()) return;
		}
		if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
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
	 * {@link #onNext(Object)} and passed to
	 * {@link #onInboundMessage(Object, IndividualInboundMessageResultObserver)}.
	 * <p>
	 * {@link #cancel(String, Throwable)} can only be called if the parent outbound observer is a
	 * {@link ClientCallStreamObserver}.</p>
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
			if (clientRequestObserver == null) {
				throw new UnsupportedOperationException("cancelling is allowed only if the"
					+ " outbound observer is a ClientCallRequestObserver");
			}
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
