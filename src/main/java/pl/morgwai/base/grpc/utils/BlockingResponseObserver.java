/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import io.grpc.stub.StreamObserver;



/**
 * Response observer for a client side that blocks until response is completed with either
 * <code>onCompleted()</code> or <code>onError(error)</code>.
 */
public class BlockingResponseObserver<T> implements StreamObserver<T> {



	public interface ResponseHandler<T> {
		void onResponse(T response);
	}

	public BlockingResponseObserver(ResponseHandler<T> responseHandler) {
		this.responseHandler = responseHandler;
	}

	ResponseHandler<T> responseHandler;

	@Override
	public void onNext(T response) {
		responseHandler.onResponse(response);
	}



	boolean completed = false;
	public boolean isCompleted() {return completed;}

	/**
	 * equivalent to {@link #awaitCompletion(long) awaitCompletion(0l)}.
	 */
	public void awaitCompletion() throws Exception { awaitCompletion(0l); }

	/**
	 * Awaits up to <code>timeoutMillis</code> for the response to be completed with
	 * {@link #onCompleted()}. Whether timeout passed or response was completed can be verified
	 * with {@link #isCompleted()}. If <code>timeoutMillis</code> is <code>0l</code> then waits
	 * indefinitely.
	 * @throws Exception if this thread was interrupted ({@link InterruptedException} in such case)
	 *     or if {@link #onError(Throwable)} was called, in which case {@link Exception#getCause()}
	 *     will return the throwable passed as argument.
	 */
	public synchronized void awaitCompletion(long timeoutMillis) throws Exception {
		if (timeoutMillis == 0l) {
			while ( ! completed) wait();
		} else 	if ( ! completed) {
			wait(timeoutMillis);
		}
		if (error != null) throw new Exception(error);
	}

	@Override
	public synchronized void onCompleted() {
		completed = true;
		notifyAll();
	}



	Throwable error;
	public Throwable getError() {return error;}

	@Override
	public void onError(Throwable error) {
		this.error = error;
		onCompleted();
	}
}
