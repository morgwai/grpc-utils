// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.StreamObservers;



/**
 * A request {@link StreamObserver} for bi-di streaming methods that dispatch work to multiple
 * threads and don't care about the order of responses. Handles all the synchronization and manual
 * flow control to maintain desired number of request messages processed concurrently and prevent
 * excessive buffering.
 * <p>
 * Typical usage:
 * <pre>
 *public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
 *        StreamObserver&lt;ResponseMessage&gt; responseObserver) {
 *    return new ConcurrentRequestObserver&lt;RequestMessage, ResponseMessage&gt;(
 *        (ServerCallStreamObserver&lt;ResponseMessage&gt;) responseObserver,
 *        executor.getMaximumPoolSize(),
 *        (requestMessage, singleRequestMessageResponseObserver) -&gt; {
 *            executor.execute(() -&gt; {
 *                var responseMessage = process(requestMessage);
 *                singleRequestMessageResponseObserver.onNext(responseMessage);
 *                singleRequestMessageResponseObserver.onCompleted();
 *            });
 *        },
 *        (error) -&gt; log.info(error)
 *    );
 *}
 * </pre></p>
 * <p>
 * Once response observers for all request messages are closed and the client closes his request
 * stream, <code>responseObserver.onCompleted()</code> is called <b>automatically</b>.</p>
 */
public class ConcurrentRequestObserver<RequestT, ResponseT> implements StreamObserver<RequestT> {



	/**
	 * Produces response messages to the given <code>requestMessage</code> and submits them to the
	 * {@code singleRequestMessageResponseObserver} (associated with this {@code requestMessage}).
	 * using {@link SingleRequestMessageResponseObserver#onNext(Object)} method.
	 * Once all response messages to the given <code>requestMessage</code> are submitted, calls
	 * {@link SingleRequestMessageResponseObserver#onCompleted()
	 * singleRequestMessageResponseObserver.onComplete()}.
	 * <p>
	 * {@code singleRequestMessageResponseObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * To avoid excessive buffering, implementations should respect
	 * {@code singleRequestMessageResponseObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() singleRequestMessageResponseObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * singleRequestMessageResponseObserver.setOnReadyHandler(...)} methods.<br/>
	 * Consider using {@link DispatchingOnReadyHandler} or
	 * {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}.</p>
	 * <p>
	 * Default implementation calls {@link #requestHandler}.</p>
	 *
	 * @see SingleRequestMessageResponseObserver
	 */
	protected void onRequestMessage(
			RequestT requestMessage,
			CallStreamObserver<ResponseT> singleRequestMessageResponseObserver) {
		requestHandler.accept(requestMessage, singleRequestMessageResponseObserver);
	}

	/**
	 * Called by {@link #onRequestMessage(Object, CallStreamObserver)}. Supplied via the param of
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructor.
	 */
	protected BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler;



	/**
	 * Default implementation calls {@link #errorHandler}.
	 * @see {@link StreamObserver#onError(Throwable)}.
	 */
	@Override
	public void onError(Throwable t) {
		errorHandler.accept(t);
	}

	/**
	 * Called by {@link #onError(Throwable)}. Supplied via the param of
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int, BiConsumer, Consumer)}
	 * constructor.
	 */
	protected Consumer<Throwable> errorHandler;



	/**
	 * Initializes {@link #requestHandler} and {@link #errorHandler} and calls
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, int)}.
	 *
	 * @param responseObserver response observer of the given gRPC method.
	 * @param requestHandler stored on {@link #requestHandler} to be called by
	 *     {@link #onRequestMessage(Object, CallStreamObserver)}.
	 * @param errorHandler stored on {@link #errorHandler} to be called by
	 *     {@link #onError(Throwable)}.
	 */
	public ConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int numberOfConcurrentRequests,
		BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		this(responseObserver, numberOfConcurrentRequests);
		this.requestHandler = requestHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * Configures flow control.
	 * Constructor for those who prefer to override
	 * {@link #onRequestMessage(Object, CallStreamObserver)} and {@link #onError(Throwable)} in a
	 * subclass instead of providing lambdas.
	 */
	protected ConcurrentRequestObserver(
			ServerCallStreamObserver<ResponseT> responseObserver,
			int numberOfConcurrentRequests) {
		this.responseObserver = responseObserver;
		responseObserver.disableAutoRequest();
		responseObserver.request(numberOfConcurrentRequests);
		responseObserver.setOnReadyHandler(() -> onResponseObserverReady());
	}



	ServerCallStreamObserver<ResponseT> responseObserver;

	boolean halfClosed = false;
	int joblessThreadCount = 0;
	final Set<SingleRequestMessageResponseObserver> ongoingRequests = new HashSet<>();

	protected Object globalLock = new Object();



	void onResponseObserverReady() {
		List<SingleRequestMessageResponseObserver> ongoingRequestsCopy;
		synchronized (globalLock) {
			// request 1 message for every thread that refrained from doing so when the buffer
			// was too full
			if (joblessThreadCount > 0 && ! halfClosed) {
				responseObserver.request(joblessThreadCount);
				joblessThreadCount = 0;
			}

			// copy ongoingRequests in case some of them get completed and try to remove themselves
			// from the set while it is iterated through below (new requests will not come thanks to
			// listener's lock)
			ongoingRequestsCopy = new ArrayList<>(ongoingRequests);
		}
		for (var individualObserver: ongoingRequestsCopy) {
			synchronized (individualObserver.onReadyHandlerLock) {
				if (individualObserver.onReadyHandler != null) {
					individualObserver.onReadyHandler.run();
				}
			}
		}
	}



	@Override
	public final void onCompleted() {
		synchronized (globalLock) {
			halfClosed = true;
			if (ongoingRequests.isEmpty()) responseObserver.onCompleted();
		}
	}



	/**
	 * Calls {@link #onRequestMessage(Object, CallStreamObserver) onRequest}({@code request},
	 * {@link #newSingleRequestMessageResponseObserver()}).
	 */
	@Override
	public final void onNext(RequestT request) {
		final var individualObserver = newSingleRequestMessageResponseObserver();
		onRequestMessage(request, individualObserver);
		synchronized (globalLock) {
			 if ( ! responseObserver.isReady()) return;
		}
		synchronized (individualObserver.onReadyHandlerLock) {
			if (individualObserver.onReadyHandler != null) {
				individualObserver.onReadyHandler.run();
			}
		}
	}

	/**
	 * Handles construction of new {@link SingleRequestMessageResponseObserver}s for subclasses to
	 * override. Called by {@link #onNext(Object)}.
	 *
	 * @see OrderedConcurrentRequestObserver#newSingleRequestMessageResponseObserver()
	 */
	protected SingleRequestMessageResponseObserver newSingleRequestMessageResponseObserver() {
		return new SingleRequestMessageResponseObserver();
	}



	/**
	 * Observer of responses to 1 particular request message. All methods are thread-safe.
	 * <p>
	 * To avoid excessive buffering, implementations of
	 * {@link ConcurrentRequestObserver#onRequestMessage(Object, CallStreamObserver)} should respect
	 * {@code singleRequestMessageResponseObserver}'s readiness with {@link #isReady()} and
	 * {@link #setOnReadyHandler(Runnable)} methods.<br/>
	 * Consider using {@link DispatchingOnReadyHandler} or
	 * {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}.</p>
	 */
	protected class SingleRequestMessageResponseObserver extends CallStreamObserver<ResponseT> {

		Runnable onReadyHandler;
		Object onReadyHandlerLock = new Object();



		SingleRequestMessageResponseObserver() {
			synchronized (globalLock) {
				ongoingRequests.add(this);
			}
		}



		/**
		 * Indicates that processing of the associated request message is completed. Once all
		 * response observer of individual request messages are completed, {@code onComplete} from
		 * the parent response observer (supplied via {@link
		 * ConcurrentRequestObserver#ConcurrentRequestObserver(ServerCallStreamObserver, int)}
		 * param) is called <b>automatically</b>.
		 */
		@Override
		public void onCompleted() {
			synchronized (globalLock) {
				if ( ! ongoingRequests.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (halfClosed && ongoingRequests.isEmpty()) {
					responseObserver.onCompleted();
					return;
				}

				if (responseObserver.isReady()) {
					responseObserver.request(1);
				} else {
					joblessThreadCount++;
				}
			}
		}



		@Override
		public void onNext(ResponseT response) {
			synchronized (globalLock) {
				if ( ! ongoingRequests.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onNext(response);
			}
		}



		/**
		 * Calls {@code onError(Throwable)} from the parent response observer. (supplied via {@link
		 * ConcurrentRequestObserver#ConcurrentRequestObserver(ServerCallStreamObserver, int)}
		 * param).
		 */
		@Override
		public void onError(Throwable t) {
			synchronized (globalLock) {
				if ( ! ongoingRequests.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onError(t);
			}
		}



		@Override
		public boolean isReady() {
			synchronized (globalLock) {
				return responseObserver.isReady();
			}
		}



		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			synchronized (onReadyHandlerLock) {
				this.onReadyHandler = onReadyHandler;
			}
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
		 * Has no effect: compression should be set using the parent response observer.
		 */
		@Override public void setMessageCompression(boolean enable) {}
	}



	static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() has been already called for this request message";
}
