// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.HashSet;
import java.util.Set;

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
 * usually the size of a threadPool to which {@link #onRequest(Object, StreamObserver)} dispatches
 * work. From then on, the observer will maintain this number of request messages being concurrently
 * processed, as long as the client can deliver them and consume responses on time and no one else
 * occupies the threadPool.<br/>
 * For example:<br/>
 * <br/>
 * <pre>
 *public StreamObserver&lt;RequestMessage&gt; myBiDiMethod(
 *        StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *    ServerCallStreamObserver&lt;ResponseMessage&gt; responseObserver =
 *            (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *
 *    var requestObserver =
 *          new ConcurrentRequestObserver&lt;RequestMessage, ResponseMessage&gt;(responseObserver) {
 *
 *        /**
 *         * Produces asynchronously 1 response message for the given requestMessage.
 *         *&sol;
 *        &commat;Override
 *        protected void onRequest(
 *                RequestT requestMessage, StreamObserver&lt;ResponseMessage&gt; responseObserver) {
 *            executor.execute(() -&gt; {
 *                var responseMessage = process(requestMessage);
 *                responseObserver.onNext(responseMessage);
 *                responseObserver.onCompleted();
 *            });
 *        }
 *
 *        &commat;Override
 *        public void onError(Throwable t) {/* ... *&sol;}
 *    }
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
public abstract class ConcurrentRequestObserver<RequestT, ResponseT>
		implements StreamObserver<RequestT> {



	/**
	 * Produces response messages to a given <code>requestMessage</code> and submits them to the
	 * <code>responseObserver</code> (associated with the given <code>requestMessage</code>).<br/>
	 * Work may be freely dispatched to several other threads. Once all response messages to a
	 * a given <code>requestMessage</code> are submitted via <code>onNext(reply)</code> method,
	 * <code>responseObserver.onComplete()</code> must be called to signal to this
	 * <code>ConcurrentRequestObserver</code> that no more response messages will be produced for
	 * this <code>requestMessage</code> and that the next one may be requested from the client
	 * (assuming server's output buffer is not too full).
	 */
	protected abstract void onRequest(
			RequestT requestMessage, StreamObserver<ResponseT> responseObserver);



	/**
	 * Creates an observer and enables manual flow control to maintain the desired concurrency
	 * level while also preventing excessive buffering of response messages.
	 */
	public ConcurrentRequestObserver(ServerCallStreamObserver<ResponseT> responseObserver) {
		this.responseObserver = responseObserver;
		responseObserver.disableAutoRequest();
		responseObserver.setOnReadyHandler(() -> onResponseObserverReady());
	}



	ServerCallStreamObserver<ResponseT> responseObserver;

	boolean halfClosed = false;

	int joblessThreadCount = 0;
	Set<RequestT> ongoingRequests = new HashSet<>();



	synchronized void onResponseObserverReady() {
		// request 1 message for every thread that refrained from doing so when buffer was too full
		if (joblessThreadCount > 0) {
			responseObserver.request(joblessThreadCount);
			joblessThreadCount = 0;
		}
	}



	@Override
	public synchronized void onCompleted() {
		halfClosed = true;
		if (ongoingRequests.isEmpty()) responseObserver.onCompleted();
	}



	@Override
	public void onNext(RequestT request) {
		onRequest(request, new OneRequestMessageResponseObserver(request));
	}



	/**
	 * Observer of responses to 1 particular request message.
	 */
	class OneRequestMessageResponseObserver implements StreamObserver<ResponseT> {

		RequestT request;



		OneRequestMessageResponseObserver(RequestT request) {
			this.request = request;
			synchronized (ConcurrentRequestObserver.this) {
				ongoingRequests.add(request);
			}
		}



		@Override
		public void onCompleted() {
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.remove(request)) {
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
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.contains(request)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onNext(response);
			}
		}



		@Override
		public void onError(Throwable t) {
			synchronized (ConcurrentRequestObserver.this) {
				if ( ! ongoingRequests.contains(request)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onError(t);
			}
		}
	}



	static final String OBSERVER_FINALIZED_MESSAGE =
			"onCompleted() has been already called for this request message";
}
