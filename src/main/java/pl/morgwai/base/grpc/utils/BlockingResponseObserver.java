// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import pl.morgwai.base.util.concurrent.Awaitable;



/**
 * A {@link ClientResponseObserver}, that blocks until the response stream is completed with either
 * {@link #onCompleted()} or {@link #onError(Throwable)}.
 * <p>
 * Typical usage:</p>
 * <pre>
 * var responseObserver = new BlockingResponseObserver&lt;ResponseMessage&gt;(response -&gt; {
 *     // handle responses here...
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
	 * The default implementation calls {@link #responseHandler}.
	 */
	@Override
	public void onNext(ResponseT response) {
		responseHandler.accept(response);
	}

	/**
	 * Called by {@link #onNext(Object)}. Initialized via {@code responseHandler}
	 * {@link #BlockingResponseObserver(Consumer, Consumer) constructor} param.
	 */
	protected final Consumer<ResponseT> responseHandler;



	/**
	 * Returns {@link ClientCallStreamObserver requestObserver} passed to
	 * {@link #beforeStart(ClientCallStreamObserver)} or {@code empty} if
	 * {@link #beforeStart(ClientCallStreamObserver)} hasn't been called yet.
	 */
	public Optional<ClientCallStreamObserver<RequestT>> getRequestObserver() {
		return Optional.ofNullable(requestObserver);
	}
	ClientCallStreamObserver<RequestT> requestObserver;

	/**
	 * The default implementation stores {@code requestObserver} (so that it can be later retrieved
	 * with {@link #getRequestObserver()}) and calls {@link #beforeStartHandler} if it's not
	 * {@code null}.
	 */
	@Override
	public void beforeStart(final ClientCallStreamObserver<RequestT> requestObserver) {
		this.requestObserver = requestObserver;
		if (beforeStartHandler != null) beforeStartHandler.accept(requestObserver);
	}

	/**
	 * Called by {@link #beforeStart(ClientCallStreamObserver)}.
	 * Initialized via {@code beforeStartHandler}
	 * {@link #BlockingResponseObserver(Consumer, Consumer) constructor} param.
	 */
	protected final Consumer<ClientCallStreamObserver<RequestT>> beforeStartHandler;



	/**
	 * Initializes {@link #responseHandler} and {@link #beforeStartHandler}.
	 */
	public BlockingResponseObserver(
			Consumer<? super ResponseT> responseHandler,
			Consumer<ClientCallStreamObserver<? super RequestT>> beforeStartHandler) {
		this.responseHandler = responseHandler != null ? responseHandler::accept: null;
		this.beforeStartHandler = beforeStartHandler != null ? beforeStartHandler::accept : null;
	}

	/**
	 * Initializes {@link #responseHandler}.
	 */
	public BlockingResponseObserver(Consumer<? super ResponseT> responseHandler) {
		this(responseHandler, null);
	}

	/**
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. At least {@link #onNext(Object)} must be overridden.
	 */
	protected BlockingResponseObserver() { this(null, null); }



	/**
	 * Returns {@code true} if either {@link #onCompleted()} or {@link #onError(Throwable)} was
	 * called, {@code false} otherwise.
	 */
	public boolean isCompleted() { return completed; }
	volatile boolean completed = false;

	/**
	 * If {@link #onError(Throwable)} has been called, returns its argument, otherwise
	 * {@code empty}.
	 */
	public Optional<Throwable> getError() { return Optional.ofNullable(error); }
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

	/**
	 * Returns {@link Awaitable} of {@link #awaitCompletion(long, TimeUnit)}.
	 */
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
	 * Notifies threads awaiting for completion via {@link #awaitCompletion(long)}.
	 */
	@Override
	public void onCompleted() {
		completed = true;
		latch.countDown();
	}



	/**
	 * Causes {@link #awaitCompletion(long)} to throw an
	 * {@link ErrorReportedException ErrorReportedException}.
	 */
	@Override
	public void onError(Throwable error) {
		this.error = error;
		onCompleted();
	}



	/**
	 * Thrown by {@link BlockingResponseObserver#awaitCompletion(long)} if
	 * {@link #onError(Throwable)} was called. {@link ErrorReportedException#getCause()} will
	 * return the exception that was passed as an argument to {@link #onError(Throwable)}.
	 */
	public static class ErrorReportedException extends Exception {
		ErrorReportedException(Throwable reportedError) { super(reportedError); }
		private static final long serialVersionUID = 1848619649489806621L;
	}
}
