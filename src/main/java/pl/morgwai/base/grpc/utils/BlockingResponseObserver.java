// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.stub.StreamObserver;



/**
 * Response observer for a client side that blocks until response is completed with either
 * {@link #onCompleted()} or {@link #onError(Throwable)}.
 * <p>
 * Typical usage:</p>
 * <pre>
 * var responseObserver = new BlockingResponseObserver&lt;ResponseMessage&gt;(response -&gt; {
 *     // handle ResponseMessage response here...
 * });
 * myGrpcServiceStub.myRemoteProcedure(someRequest, responseObserver);
 * try {
 *     responseObserver.awaitCompletion();
 *     // continue positive flow here...
 * } catch (InterruptedException e) {  // often unreachable code
 * } catch (ErrorReportedException e) {
 *     Throwable reportedError = e.getCause();
 *     // handle error that was reported via onError(reportedError) here...
 * }</pre>
 */
public class BlockingResponseObserver<T> implements StreamObserver<T> {



	/**
	 * Called by {@link #onNext(Object)}.
	 */
	protected Consumer<T> responseHandler;

	/**
	 * Initializes {@link #responseHandler}.
	 */
	public BlockingResponseObserver(Consumer<T> responseHandler) {
		this.responseHandler = responseHandler;
	}

	/**
	 * Calls {@link #responseHandler}.
	 */
	@Override
	public void onNext(T response) {
		responseHandler.accept(response);
	}

	/**
	 * Constructor for those who prefer to override {@link #onNext(Object)} in a subclass instead
	 * of providing a lambda.
	 */
	protected BlockingResponseObserver() {}



	/**
	 * Returns {@code true} if either {@link #onCompleted()} or {@link #onError(Throwable)} was
	 * called.
	 */
	public boolean isCompleted() { return completed; }
	volatile boolean completed = false;

	/**
	 * If {@link #onError(Throwable)} has been called, returns its argument, otherwise {@code null}.
	 */
	public Throwable getError() { return error; }
	volatile Throwable error;

	final CountDownLatch latch = new CountDownLatch(1);



	/**
	 * Awaits up to {@code timeoutMillis} for {@link #onCompleted()} or {@link #onError(Throwable)}
	 * to be called. If {@code timeoutMillis} is {@code 0} then waits without a timeout.
	 * @return {@code true} if {@link #onCompleted()} was called, {@code false} if timeout passed
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     <code>getCause()</code> will return the throwable passed as argument.
	 */
	public boolean awaitCompletion(long timeoutMillis)
			throws ErrorReportedException, InterruptedException {
		if (timeoutMillis == 0l) {
			latch.await();
		} else {
			latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
		}
		if (error != null) throw new ErrorReportedException(error);
		return completed;
	}

	/**
	 * Equivalent to {@link #awaitCompletion(long) awaitCompletion(0l)}.
	 */
	public void awaitCompletion() throws ErrorReportedException, InterruptedException {
		latch.await();
		if (error != null) throw new ErrorReportedException(error);
	}



	/**
	 * Notifies the thread that called {@link #awaitCompletion(long)}.
	 */
	@Override
	public void onCompleted() {
		completed = true;
		latch.countDown();
	}



	/**
	 * Causes {@link #awaitCompletion(long)} to throw a {@link ErrorReportedException}.
	 */
	@Override
	public void onError(Throwable error) {
		this.error = error;
		onCompleted();
	}



	/**
	 * Thrown by {@link BlockingResponseObserver#awaitCompletion(long)}. {@code getCause()} will
	 * return exception reported via {@link BlockingResponseObserver#onError(Throwable)}.
	 */
	public static class ErrorReportedException extends Exception {
		ErrorReportedException(Throwable reportedError) { super(reportedError); }
		private static final long serialVersionUID = 4822900070324973236L;
	}
}
