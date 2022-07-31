// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



/**
 * A request {@link StreamObserver} for bi-di streaming methods that may dispatch work to multiple
 * threads and don't care about the order of responses. Handles all the synchronization and manual
 * flow control to maintain desired number of request messages processed concurrently and prevent
 * excessive buffering.
 * <p>
 * Typical usage:</p>
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
 * Once response observers for all request messages are closed and the client closes his request
 * stream, <code>responseObserver.onCompleted()</code> is called <b>automatically</b>.</p>
 * <p>
 * Note that it is totally fine to not dispatch processing to other threads: this will result in
 * sequential processing of requests with respect to flow-control.
 * {@code numberOfInitiallyRequestedMessages} constructor param should usually be set to 1 in such
 * cases.</p>
 * <p>
 * If one request message may produce multiple responses, this class can be combined with utilities
 * like {@link StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)} or
 * {@link DispatchingOnReadyHandler}:</p>
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
 *         (requestMessage, individualRequestMessageResponseObserver) -&gt; executor.execute(
 *             () -&gt; StreamObservers.copyWithFlowControl(
 *                 new RequestProcessor(requestMessage),
 *                 individualRequestMessageResponseObserver
 *             )
 *         ),
 *         (error) -&gt; log.info(error)
 *     );
 * }</pre>
 */
public class ConcurrentRequestObserver<RequestT, ResponseT> implements StreamObserver<RequestT> {



	/**
	 * Produces response messages to the given {@code requestMessage}. Responses must be submitted
	 * to {@code individualRequestMessageResponseObserver} that is associated with this
	 * {@code requestMessage} using {@link CallStreamObserver#onNext(Object)
	 * individualRequestMessageResponseObserver.onNext(response)}.
	 * Once all responses to this {@code requestMessage} are submitted, this method must call
	 * {@link IndividualRequestMessageResponseObserver#onCompleted()
	 * individualRequestMessageResponseObserver.onComplete()}.
	 * <p>
	 * {@code individualRequestMessageResponseObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * To avoid excessive buffering, implementations should respect
	 * {@code individualRequestMessageResponseObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() individualRequestMessageResponseObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * individualRequestMessageResponseObserver.setOnReadyHandler(...)} methods.<br/>
	 * Consider using {@link DispatchingOnReadyHandler} or
	 * {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}.</p>
	 * <p>
	 * Default implementation calls {@link #requestHandler}.</p>
	 *
	 * @see IndividualRequestMessageResponseObserver
	 */
	protected void onRequestMessage(
			RequestT requestMessage,
			CallStreamObserver<ResponseT> individualRequestMessageResponseObserver) {
		requestHandler.accept(requestMessage, individualRequestMessageResponseObserver);
	}

	/**
	 * Called by {@link #onRequestMessage(Object, CallStreamObserver)}. Initialized via the param of
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructor.
	 */
	protected BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler;



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
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructor.
	 */
	protected Consumer<Throwable> errorHandler;



	/**
	 * Initializes {@link #requestHandler} and {@link #errorHandler} and calls
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int)}.
	 *
	 * @param requestHandler stored on {@link #requestHandler} to be called by
	 *     {@link #onRequestMessage(Object, CallStreamObserver)}.
	 * @param errorHandler stored on {@link #errorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 *
	 * @see #ConcurrentRequestObserver(ServerCallStreamObserver, int) The other constructor for
	 *     the meaning of the remaining params.
	 */
	public ConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int numberOfInitiallyRequestedMessages,
		BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		this(responseObserver, numberOfInitiallyRequestedMessages);
		this.requestHandler = requestHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * Configures flow control.
	 * Constructor for those who prefer to override
	 * {@link #onRequestMessage(Object, CallStreamObserver)} and {@link #onError(Throwable)} in a
	 * subclass instead of providing lambdas.
	 *
	 * @param responseObserver response observer of the given gRPC method.
	 * @param numberOfInitiallyRequestedMessages the constructed observer will call
	 *     {@code responseObserver.request(numberOfInitiallyRequestedMessages)}. If
	 *     {@link #onRequestMessage(Object, CallStreamObserver)} dispatches work to other threads,
	 *     rather than processing synchronously, this will be the maximum number of request messages
	 *     processed concurrently. It should correspond to server's concurrent processing
	 *     capabilities.
	 */
	protected ConcurrentRequestObserver(
			ServerCallStreamObserver<ResponseT> responseObserver,
			int numberOfInitiallyRequestedMessages) {
		this.responseObserver = responseObserver;
		responseObserver.disableAutoRequest();
		if (numberOfInitiallyRequestedMessages > 0) {
			responseObserver.request(numberOfInitiallyRequestedMessages);
		}
		responseObserver.setOnReadyHandler(this::onReady);
	}



	final ServerCallStreamObserver<ResponseT> responseObserver;

	boolean halfClosed = false;
	int idleCount = 0;
	final Set<IndividualRequestMessageResponseObserver> ongoingRequests =
			ConcurrentHashMap.newKeySet();

	protected final Object lock = new Object();



	void onReady() {
		synchronized (lock) {
			if (idleCount > 0 && !halfClosed) {
				responseObserver.request(idleCount);
				idleCount = 0;
			}
		}
		for (var individualObserver: ongoingRequests) {
			// a new request can't arrive now thanks to Listener's concurrency contract
			if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
		}
	}



	@Override
	public final void onCompleted() {
		synchronized (lock) {
			halfClosed = true;
			if (ongoingRequests.isEmpty()) responseObserver.onCompleted();
		}
	}



	/**
	 * Calls {@link #onRequestMessage(Object, CallStreamObserver) onRequest}({@code request},
	 * {@link #newIndividualObserver()}).
	 */
	@Override
	public final void onNext(RequestT request) {
		final var individualObserver = newIndividualObserver();
		onRequestMessage(request, individualObserver);
		synchronized (lock) {
			if ( !responseObserver.isReady()) return;
		}
		if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
	}

	/**
	 * Constructs new
	 * {@link IndividualRequestMessageResponseObserver IndividualRequestMessageResponseObservers}
	 * for {@link #onNext(Object)} method. Subclasses may override this method if they need to use
	 * specialized subclasses of
	 * {@link IndividualRequestMessageResponseObserver IndividualRequestMessageResponseObserver}.
	 * @see OrderedConcurrentRequestObserver#newIndividualObserver()
	 */
	protected IndividualRequestMessageResponseObserver newIndividualObserver() {
		return new IndividualRequestMessageResponseObserver();
	}



	/**
	 * Observer of responses to 1 particular request message. All methods are thread-safe. A new
	 * instance is created each time a new message arrives via {@link #onNext(Object)} and passed
	 * to {@link #onRequestMessage(Object, CallStreamObserver)}.
	 */
	protected class IndividualRequestMessageResponseObserver extends CallStreamObserver<ResponseT> {

		volatile Runnable onReadyHandler;



		protected IndividualRequestMessageResponseObserver() {
			ongoingRequests.add(this);
		}



		/**
		 * Indicates that processing of the associated request message is completed. Once all
		 * individual observers are completed and client have half-closed,
		 * {@link ConcurrentRequestObserver#onCompleted() onCompleted() from the parent response
		 * observer} is called automatically.
		 */
		@Override
		public void onCompleted() {
			synchronized (lock) {
				if ( !ongoingRequests.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (halfClosed && ongoingRequests.isEmpty()) {
					responseObserver.onCompleted();
					return;
				}

				if (responseObserver.isReady()) {
					responseObserver.request(1);
				} else {
					idleCount++;
				}
			}
		}



		@Override
		public void onNext(ResponseT response) {
			synchronized (lock) {
				if ( !ongoingRequests.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onNext(response);
			}
		}



		/**
		 * Calls {@link ConcurrentRequestObserver#onError(Throwable) onError(error) from the
		 * parent response observer}.
		 */
		@Override
		public void onError(Throwable error) {
			synchronized (lock) {
				if ( !ongoingRequests.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onError(error);
			}
		}



		@Override
		public boolean isReady() {
			synchronized (lock) {
				return responseObserver.isReady();
			}
		}



		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			this.onReadyHandler = onReadyHandler;
		}



		/**
		 * Has no effect: request messages are requested automatically by the parent
		 * {@link ConcurrentRequestObserver}.
		 */
		@Override public void disableAutoInboundFlowControl() {}

		/**
		 * Has no effect: request messages are requested automatically by the parent
		 * {@link ConcurrentRequestObserver}.
		 */
		@Override public void request(int count) {}

		/**
		 * Has no effect: compression should be set using the parent
		 * {@link ConcurrentRequestObserver}.
		 */
		@Override public void setMessageCompression(boolean enable) {}
	}



	static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() or onError() has been already called for this observer";
}
