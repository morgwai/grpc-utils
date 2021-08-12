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



/**
 * A request <code>StreamObserver</code> for bi-di streaming methods that dispatch work to other
 * threads and don't need to preserve order of responses. Handles all the synchronization and manual
 * flow control to maintain desired level of concurrency and prevent excessive buffering.<br/>
 * <br/>
 * After creating an observer, but before returning it from a method, a delivery of <code>n</code>
 * request messages should be requested via {@link io.grpc.stub.CallStreamObserver#request(int)
 * responseObserver.request(n)} method, where <code>n</code> is the desired level of concurrency,
 * usually the size of a threadPool to which {@link #onRequest(Object, CallStreamObserver)}
 * dispatches work. From then on, the observer will maintain this number of request messages being
 * concurrently processed, as long as the client can deliver them and consume responses on time and
 * no one else occupies the threadPool.<br/>
 * For example:<br/>
 * <br/>
 * <pre>
 *public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
 *        StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *    ServerCallStreamObserver&lt;ResponseMessage&gt; responseObserver =
 *            (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *
 *    var requestObserver = new ConcurrentRequestObserver&lt;RequestMessage, ResponseMessage&gt;(
 *        responseObserver,
 *        (requestMessage, singleRequestMessageResponseObserver) -&gt; {
 *            executor.execute(() -&gt; {
 *                var responseMessage = process(requestMessage);
 *                singleRequestMessageResponseObserver.onNext(responseMessage);
 *                singleRequestMessageResponseObserver.onCompleted();
 *            });
 *        },
 *        (error) -&gt; log.info(error)
 *    );
 *
 *    responseObserver.request(10);  // 10 is the size of executor's threadPool
 *    return requestObserver;
 *}
 * </pre>
 * If <code>1</code> is requested initially, then although handled asynchronously by executor
 * threads, request messages will be handled sequentially and thus the order of response messages
 * will correspond to request messages.<br/>
 * <br/>
 * Once response observers for all request messages are closed and the client closes his request
 * stream, <code>responseObserver.onCompleted()</code> is called <b>automatically</b>.
 */
public class ConcurrentRequestObserver<RequestT, ResponseT>
		implements StreamObserver<RequestT> {



	/**
	 * Produces response messages to a given <code>requestMessage</code> and submits them to the
	 * {@code singleRequestMessageResponseObserver} (associated with the given
	 * {@code requestMessage}).<br/>
	 * Work may be freely dispatched to several other threads. Once all response messages to a
	 * a given <code>requestMessage</code> are submitted via {@code onNext(reply)} method,
	 * <code>singleRequestMessageResponseObserver.onComplete()</code> must be called to signal to
	 * this <code>ConcurrentRequestObserver</code> that no more response messages will be produced
	 * for this <code>requestMessage</code> and that the next one may be requested from the client
	 * (assuming server's output buffer is not too full).<br/>
	 * <br/>
	 * This implementation calls {@link #requestHandler} supplied via the param of
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, BiConsumer, Consumer)}
	 * constructor.
	 */
	protected void onRequest(
			RequestT requestMessage,
			CallStreamObserver<ResponseT> singleRequestMessageResponseObserver) {
		requestHandler.accept(requestMessage, singleRequestMessageResponseObserver);
	}

	protected BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler;



	/**
	 * See {@link StreamObserver#onError(Throwable)} for details.
	 * This implementation calls {@link #errorHandler} supplied via the param of
	 * {@link #ConcurrentRequestObserver(ServerCallStreamObserver, BiConsumer, Consumer)}
	 * constructor.
	 */
	@Override
	public void onError(Throwable t) {
		errorHandler.accept(t);
	}

	protected Consumer<Throwable> errorHandler;



	/**
	 * Creates an observer and enables manual flow control to maintain the desired concurrency
	 * level while also preventing excessive buffering of response messages.
	 *
	 * @param responseObserver
	 * @param requestHandler lambda called by {@link #onRequest(Object, CallStreamObserver)}
	 * @param errorHandler lambda called by {@link #onError(Throwable)}
	 */
	public ConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		this(responseObserver);
		this.requestHandler = requestHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * Constructor for those who prefer to override {@link #onRequest(Object, CallStreamObserver)}
	 * and {@link #onError(Throwable)} in a subclass instead of providing lambdas.
	 */
	protected ConcurrentRequestObserver(ServerCallStreamObserver<ResponseT> responseObserver) {
		this.responseObserver = responseObserver;
		responseObserver.disableAutoRequest();
		responseObserver.setOnReadyHandler(() -> onResponseObserverReady());
	}

	ServerCallStreamObserver<ResponseT> responseObserver;



	boolean halfClosed = false;
	int joblessThreadCount = 0;
	Set<SingleRequestMessageResponseObserver> ongoingRequests = new HashSet<>();



	void onResponseObserverReady() {
		List<SingleRequestMessageResponseObserver> ongoingRequestsCopy;
		synchronized (this) {
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
			synchronized (individualObserver) {
				if (individualObserver.onReadyHandler != null) {
					individualObserver.onReadyHandler.run();
				}
			}
		}
	}



	@Override
	public synchronized void onCompleted() {
		halfClosed = true;
		if (ongoingRequests.isEmpty()) responseObserver.onCompleted();
	}



	@Override
	public void onNext(RequestT request) {
		var individualObserver = newSingleRequestMessageResponseObserver();
		onRequest(request, individualObserver);
		synchronized (individualObserver) {
			if (individualObserver.onReadyHandler != null) {
				individualObserver.onReadyHandler.run();
			}
		}
	}

	protected SingleRequestMessageResponseObserver newSingleRequestMessageResponseObserver() {
		return new SingleRequestMessageResponseObserver();
	}



	/**
	 * Observer of responses to 1 particular request message.
	 */
	class SingleRequestMessageResponseObserver extends CallStreamObserver<ResponseT> {

		Runnable onReadyHandler;



		SingleRequestMessageResponseObserver() {
			synchronized (ConcurrentRequestObserver.this) {
				ongoingRequests.add(this);
			}
		}



		@Override
		public void onCompleted() {
			boolean ready;
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.remove(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				if (halfClosed && ongoingRequests.isEmpty()) {
					responseObserver.onCompleted();
					return;
				}

				ready = responseObserver.isReady();
				if ( ! ready) joblessThreadCount++;
			}
			if (ready) responseObserver.request(1);
		}



		@Override
		public void onNext(ResponseT response) {
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onNext(response);
			}
		}



		@Override
		public void onError(Throwable t) {
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.contains(this)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onError(t);
			}
		}



		@Override
		public boolean isReady() {
			synchronized (ConcurrentRequestObserver.this) {
				return responseObserver.isReady();
			}
		}



		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			synchronized (this) {
				this.onReadyHandler = onReadyHandler;
			}
		}



		@Override public void disableAutoInboundFlowControl() {}

		@Override public void request(int count) {}

		@Override public void setMessageCompression(boolean enable) {}
	}



	static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() has been already called for this request message";
}
