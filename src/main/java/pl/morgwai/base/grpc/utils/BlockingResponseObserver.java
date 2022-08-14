// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import pl.morgwai.base.concurrent.Awaitable;



/**
 * A {@link ClientResponseObserver}, that blocks until response is completed with either
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
	 * Default implementation calls {@link #responseHandler}.
	 */
	@Override
	public void onNext(ResponseT response) {
		responseHandler.accept(response);
	}

	/**
	 * Called by {@link #onNext(Object)}. Initialized via the constructor param.
	 */
	protected Consumer<ResponseT> responseHandler;



	/**
	 * Returns {@link ClientCallStreamObserver requestObserver} passed to
	 * {@link #beforeStart(ClientCallStreamObserver)}.
	 */
	public ClientCallStreamObserver<RequestT> getRequestObserver() { return requestObserver; }
	protected ClientCallStreamObserver<RequestT> requestObserver;

	/**
	 * Default implementation stores {@code requestObserver} (so that it can be later retrieved with
	 * {@link #getRequestObserver()}) and calls {@link #startHandler}.
	 */
	@Override
	public void beforeStart(final ClientCallStreamObserver<RequestT> requestObserver) {
		this.requestObserver = requestObserver;
		if (startHandler != null) startHandler.accept(requestObserver);
	}

	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver)}.
	 * Initialized via the constructor param.
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
	 * Awaits up to {@code timeout} of {@code unit} for {@link #onCompleted()} or
	 * {@link #onError(Throwable)} to be called.
	 * @return {@code true} if {@link #onCompleted()} was called, {@code false} if timeout passed
	 * @throws ErrorReportedException if {@link #onError(Throwable)} was called.
	 *     {@link ErrorReportedException#getCause() getCause()} will return the throwable passed as
	 *     the argument.
	 */
	public boolean awaitCompletion(long timeout, TimeUnit unit)
			throws ErrorReportedException, InterruptedException {
		latch.await(timeout, unit);
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
	 * Awaits without a timeout for {@link #onCompleted()} or {@link #onError(Throwable)} to be
	 * called.
	 * @see #awaitCompletion(long, TimeUnit)
	 */
	public void awaitCompletion() throws ErrorReportedException, InterruptedException {
		latch.await();
		if (error != null) throw new ErrorReportedException(error);
	}

	public Awaitable.WithUnit toAwaitable() {
		return (timeout, unit) -> {
			try {
				return awaitCompletion(timeout, unit);
			} catch (ErrorReportedException e) {
				return true;
			}
		};
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
		private static final long serialVersionUID = 1848619649489806621L;
	}
}
