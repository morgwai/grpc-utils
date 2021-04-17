/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import static pl.morgwai.base.grpc.utils.AsyncSequentialOneToOneRequestObserver.State.*;



/**
 * A request <code>StreamObserver</code> for async bi-di streaming methods that send 1 response
 * message for each 1 request message. Takes care of all synchronization and message flow control:
 * user needs only to implement {@link #onNextMessage(Object, OneResponseObserver)}.
 */
public abstract class AsyncSequentialOneToOneRequestObserver<RequestT, ResponseT>
		implements StreamObserver<RequestT> {



	/**
	 * Must produce 1 response message for the received <code>requestMessage</code> and submit it
	 * to the <code>responseObserver</code>. May dispatch work to other threads without any
	 * limitations: the only requirement is that either
	 * {@link OneResponseObserver#onResponse(Object)} or
	 * {@link OneResponseObserver#onError(Throwable)} gets called eventually.
	 */
	protected abstract void onNext(
			RequestT requestMessage, OneResponseObserver<ResponseT> responseObserver);

	/**
	 * A response observer expecting exactly 1 message or error.
	 */
	public interface OneResponseObserver<ResponseT> {
		void onResponse(ResponseT response);
		public void onError(Throwable t);
	}



	enum State {

		/**
		 * A message is currently being actively processed.
		 * After the processing is done, if <code>responseObserver</code>'s buffer is ready, then 1
		 * new message will be requested immediately and state will be switched to
		 * {@link #AWAITING}. If the buffer is full, then  the state will be switched to
		 * {@link #BUFFER_FULL}.
		 */
		PROCESSING,

		/**
		 * <code>responseObserver</code>'s buffer is ready, so 1 new message has been requested from
		 * the client and its delivery is being awaited.
		 * When a message arrives, <code>onNext(...)</code> will be called and the state will be
		 * switched to {@link #PROCESSING}.
		 */
		AWAITING,

		/**
		 * <code>responseObserver</code>'s buffer is full, so
		 * {@link AsyncSequentialOneToOneRequestObserver#requestAtMostOneIfNotProcessing()
		 * a callback signaling it's readiness} is being awaited. The callback will request 1 new
		 * message from the client and the state will be switched to {@link #AWAITING}.
		 */
		BUFFER_FULL
	}
	State state;

	/**
	 * Indicates if the client has signaled the end of his stream. Switched by
	 * {@link #onCompleted()}.
	 */
	boolean halfClosed;

	ServerCallStreamObserver<ResponseT> responseObserver;



	/**
	 * Switches state to {@link State#PROCESSING} and calls user overridden
	 * {@link #onNext(Object, OneResponseObserver)} with {@link #responseObserver} wrapped in
	 * {@link OneResponseObserver}.
	 */
	@Override
	public void onNext(RequestT message) {
		synchronized (this) {
			state = PROCESSING;
		}
		onNext(message, new OneResponseObserver<ResponseT>() {

			AtomicBoolean finilized = new AtomicBoolean(false);

			/**
			 * Submits <code>response</code> to the underlying <code>responseObserver</code> and
			 * performs bookkeeping.
			 * If client has already signaled the end of his stream, calls
			 * <code>responseObserver.onCompleted()</code>.
			 * If not, then  depending on buffer state, either switches to {@link State#BUFFER_FULL}
			 * or requests 1 next messages and switches to {@link State#AWAITING}.
			 */
			@Override
			public void onResponse(ResponseT response) {
				if ( ! finilized.compareAndSet(false, true)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onNext(response);
				synchronized (AsyncSequentialOneToOneRequestObserver.this) {
					if (halfClosed) {
						responseObserver.onCompleted();
						return;
					}
					if (responseObserver.isReady()) {
						state = AWAITING;
						responseObserver.request(1);
					} else {
						state = BUFFER_FULL;
					}
				}
			}

			@Override
			public void onError(Throwable t) {
				if ( ! finilized.compareAndSet(false, true)) {
					throw new IllegalStateException(OBSERVER_FINALIZED_MESSAGE);
				}
				responseObserver.onError(t);
			}
		});
	}



	@Override
	public synchronized void onCompleted() {
		halfClosed = true;
		if (state != PROCESSING) responseObserver.onCompleted();
	}



	/**
	 * responseObserver.onReadyHandler
	 */
	synchronized void requestAtMostOneIfNotProcessing () {
		if (state == BUFFER_FULL) {
			state = AWAITING;
			responseObserver.request(1);
		}
	}



	public AsyncSequentialOneToOneRequestObserver(StreamObserver<ResponseT> basicResponseObserver) {
		halfClosed = false;
		responseObserver = (ServerCallStreamObserver<ResponseT>) basicResponseObserver;
		responseObserver.disableAutoRequest();
		responseObserver.setOnReadyHandler(() -> requestAtMostOneIfNotProcessing());

		state = AWAITING;
		responseObserver.request(1);
	}



	static final String OBSERVER_FINALIZED_MESSAGE = "observer already finalized";
}
