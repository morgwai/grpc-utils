// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.Consumer;

import io.grpc.stub.StreamObserver;



/**
 * Response observer for a client side that blocks until response is completed with either
 * {@link #onCompleted()} or {@link #onError(Throwable)}.
 * <p>
 * Typical usage:
 * <pre>
 *var responseObserver = new BlockingResponseObserver&lt;ResponseMessage&gt;(response -&gt; {
 *    // handle ResponseMessage response here...
 *});
 *myGrpcServiceStub.myRemoteProcedure(someRequest, responseObserver);
 *try {
 *    responseObserver.awaitCompletion();
 *    // continue positive flow here...
 *} catch (InterruptedException e) {  // often unreachable code
 *} catch (ErrorReportedException e) {
 *    Throwable reportedError = e.getCause();
 *    // handle error that was reported via onError(reportedError) here...
 *}
 * </pre></p>
 */
public class BlockingResponseObserver<T> implements StreamObserver<T> {



	@Override
	public void onNext(T response) {
		responseHandler.accept(response);
	}

	public BlockingResponseObserver(Consumer<T> responseHandler) {
		this.responseHandler = responseHandler;
	}

	protected Consumer<T> responseHandler;



	/**
	 * Constructor for those who prefer to override {@link #onNext(Object)} in a subclass instead
	 * of providing a lambda.
	 */
	protected BlockingResponseObserver() {}



	/**
	 * Awaits up to <code>timeoutMillis</code> for {@link #onCompleted()} or
	 * {@link #onError(Throwable)} to be called.
	 * @return {@code true} if {@link #onCompleted()} was called, {@code false} if timeout passed
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     <code>getCause()</code> will return the throwable passed as argument.
	 */
	public synchronized boolean awaitCompletion(long timeoutMillis)
			throws ErrorReportedException, InterruptedException {
		if (timeoutMillis == 0l) {
			while ( ! completed) wait();
		} else {
			if ( ! completed) wait(timeoutMillis);
		}
		if (error != null) throw new ErrorReportedException(error);
		return completed;
	}

	boolean completed = false;
	public boolean isCompleted() { return completed; }

	/**
	 * Awaits for {@link #onCompleted()} or {@link #onError(Throwable)} to be called.
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     <code>getCause()</code> will return the throwable passed as argument.
	 */
	public void awaitCompletion() throws ErrorReportedException, InterruptedException {
		awaitCompletion(0l);
	}



	@Override
	public synchronized void onCompleted() {
		completed = true;
		notifyAll();
	}



	@Override
	public void onError(Throwable error) {
		this.error = error;
		onCompleted();
	}

	Throwable error;
	public Throwable getError() { return error; }



	public static class ErrorReportedException extends Exception {
		ErrorReportedException(Throwable reportedError) { super(reportedError); }
		private static final long serialVersionUID = 4822900070324973236L;
	}
}
