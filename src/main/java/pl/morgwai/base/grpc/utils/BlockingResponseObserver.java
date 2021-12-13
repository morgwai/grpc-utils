// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;



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
public class BlockingResponseObserver<RequestT, ResponseT>
		implements ClientResponseObserver<RequestT, ResponseT> {



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

	/**
	 * Returns {@link ClientCallStreamObserver requestObserver} passed to
	 * {@link #beforeStart(ClientCallStreamObserver)}.
	 */
	public ClientCallStreamObserver<RequestT> getRequestObserver() { return requestObserver; }
	ClientCallStreamObserver<RequestT> requestObserver;

	/**
	 * Called by {@link #onNext(Object)}.
	 */
	protected Consumer<ResponseT> responseHandler;

	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver)}.
	 */
	protected Consumer<ClientCallStreamObserver<RequestT>> startHandler;



	/**
	 * Initializes {@link #responseHandler}.
	 */
	public BlockingResponseObserver(Consumer<ResponseT> responseHandler) {
		this.responseHandler = responseHandler;
	}

	/**
	 * Initializes {@link #responseHandler} and {@link #startHandler}.
	 */
	public BlockingResponseObserver(
			Consumer<ResponseT> responseHandler,
			Consumer<ClientCallStreamObserver<RequestT>> startHandler) {
		this.responseHandler = responseHandler;
		this.startHandler = startHandler;
	}



	/**
	 * Stores {@code requestObserver} (so that it can be later retrieved with
	 * {@link #getRequestObserver()}) and calls {@link #startHandler}.
	 */
	@Override
	public final void beforeStart(final ClientCallStreamObserver<RequestT> requestObserver) {
		this.requestObserver = requestObserver;
		if (startHandler != null) startHandler.accept(requestObserver);
	}



	/**
	 * Calls {@link #responseHandler}. If the handler throws anything, the call will be
	 * {@link ClientCallStreamObserver#cancel(String, Throwable) cancelled}.
	 */
	@Override
	public void onNext(ResponseT response) {
		try {
			responseHandler.accept(response);
		} catch (Throwable t) {
			requestObserver.cancel(null, t);
			if (t instanceof Error) throw (Error) t;
		}
	}



	/**
	 * Constructor for those who prefer to override {@link #onNext(Object)} in a subclass instead
	 * of providing a lambda.
	 */
	protected BlockingResponseObserver() {}



	final CountDownLatch latch = new CountDownLatch(1);



	/**
	 * Awaits up to {@code timeout} of {@code unit} for {@link #onCompleted()} or
	 * {@link #onError(Throwable)} to be called. If {@code timeout} is {@code 0} then awaits without
	 * a timeout.
	 * @return {@code true} if {@link #onCompleted()} was called, {@code false} if timeout passed
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     <code>getCause()</code> will return the throwable passed as argument.
	 */
	public boolean awaitCompletion(long timeout, TimeUnit unit)
			throws ErrorReportedException, InterruptedException {
		if (timeout == 0l) {
			latch.await();
		} else {
			latch.await(timeout, unit);
		}
		if (error != null) throw new ErrorReportedException(error);
		return completed;
	}

	/**
	 * Calls {@link #awaitCompletion(long, TimeUnit)
	 * awaitCompletion(timeoutMillis, TimeUnit.MILLISECONDS)}.
	 */
	public boolean awaitCompletion(long timeoutMillis)
			throws ErrorReportedException, InterruptedException {
		return awaitCompletion(timeoutMillis, TimeUnit.MILLISECONDS);
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
