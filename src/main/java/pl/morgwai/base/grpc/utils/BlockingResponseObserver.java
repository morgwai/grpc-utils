/*
 * Copyright (c) Piotr Morgwai Kotarbinski
 */
package pl.morgwai.base.grpc.utils;

import io.grpc.stub.StreamObserver;



/**
 * Response observer for a client side that blocks until response is completed with either
 * <code>onCompleted()</code> or <code>onError(error)</code>.
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
 *    // handle error reported via onError(reportedError) here...
 *}
 * </pre>
 */
public class BlockingResponseObserver<T> implements StreamObserver<T> {



	/**
	 * Functional interface for lambdas implementing <code>onNext(msg)</code> passed as an argument
	 * to {@link BlockingResponseObserver#BlockingResponseObserver(ResponseHandler)}.
	 */
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
	public void awaitCompletion() throws ErrorReportedException, InterruptedException {
		awaitCompletion(0l);
	}

	/**
	 * Awaits up to <code>timeoutMillis</code> for the response to be completed with
	 * {@link #onCompleted()}. Whether timeout passed or response was completed can be verified
	 * with {@link #isCompleted()}. If <code>timeoutMillis</code> is <code>0l</code> then waits
	 * indefinitely.
	 * @throws InterruptedException if this thread gets interrupted.
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     <code>getCause()</code> will return the throwable passed as argument.
	 */
	public synchronized void awaitCompletion(long timeoutMillis)
			throws ErrorReportedException, InterruptedException {
		if (timeoutMillis == 0l) {
			while ( ! completed) wait();
		} else 	if ( ! completed) {
			wait(timeoutMillis);
		}
		if (error != null) throw new ErrorReportedException(error);
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

	public static class ErrorReportedException extends Exception {
		ErrorReportedException(Throwable reportedError) {super(reportedError);}
		private static final long serialVersionUID = 4822900070324973236L;
	}
}
