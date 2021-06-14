// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Function;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Status.Code;
import io.grpc.stub.ServerCallStreamObserver;



/**
 * Handles server streaming calls that need to dispatch CPU-intensive response producing to an
 * external executor. Calling {@link #handle()} will eventually have the same effects as if the
 * below code was dispatched to {@link #cpuIntensiveOpExecutor}:
 * <pre>
 *try {
 *    while ( ! completionIndicator.call())
 *        responseObserver.onNext(responseProducer.call());
 *    responseObserver.onCompleted();
 *} catch (Throwable t) {
 *    try {
 *        responseObserver.onError(Status.INTERNAL.withCause(t).asException());
 *    } catch (Throwable t2) {
 *        t.addSuppressed(t2);
 *    }
 *    exceptionHandler.apply(t);
 *    if (t instanceof Error) throw (Error) t;
 *} finally {
 *    cleanupHandler.run();
 *}
 * </pre>
 * However, the work is automatically suspended/resumed whenever {@link #responseObserver} becomes
 * unready/ready <b>without</b> blocking executor's thread.<br/>
 * If a client cancels a call, {@link #exceptionHandler} will <b>not</b> be called:
 * {@link ServerCallStreamObserver#setOnCancelHandler(Runnable)} should be used instead
 * ({@link #cleanupHandler} will be called regardless).
 */
public class CpuIntensiveServerStreamingCallHandler<RequestT, ResponseT> {



	ServerCallStreamObserver<ResponseT> responseObserver;
	Executor cpuIntensiveOpExecutor;
	Callable<Boolean> completionIndicator;
	Function<Throwable, Void> exceptionHandler;
	Runnable cleanupHandler;
	Callable<ResponseT> responseProducer;

	boolean processingInProgress = false;



	/**
	 * Sets <code>onReadyHandler</code> which results in call to be handled after a given user RPC
	 * method exits.
	 */
	public void handle() {
		responseObserver.setOnReadyHandler(() -> {
			synchronized (responseObserver) {
				if (processingInProgress) return;
				processingInProgress = true;
			}
			cpuIntensiveOpExecutor.execute(handler);
		});
	}

	/**
	 * Handles 1 cycle of {@link #responseObserver}'s readiness.
	 */
	Runnable handler = () -> {
		try {
			while (true) {
				if (responseObserver.isCancelled()) return;
				synchronized (responseObserver) {
					if ( ! responseObserver.isReady()) {
						processingInProgress = false;
						return;
					}
				}
				if (completionIndicator.call()) {
					responseObserver.onCompleted();
					return;
				}

				responseObserver.onNext(responseProducer.call());
			}
		} catch (StatusRuntimeException e) {
			if (e.getStatus().getCode() != Code.CANCELLED) exceptionHandler.apply(e);
		} catch (Throwable t) {
			try {
				responseObserver.onError(Status.INTERNAL.withCause(t).asException());
			} catch (Throwable t2) {
				t.addSuppressed(t2);
			}
			exceptionHandler.apply(t);
			if (t instanceof Error) throw (Error) t;
		} finally {
			cleanupHandler.run();
		}
	};



	public CpuIntensiveServerStreamingCallHandler(
		ServerCallStreamObserver<ResponseT> responseObserver,
		Executor cpuIntensiveOpExecutor,
		Callable<Boolean> completionIndicator,
		Function<Throwable, Void> exceptionHandler,
		Runnable cleanupHandler,
		Callable<ResponseT> responseProducer
	) {
		this.responseObserver = responseObserver;
		this.cpuIntensiveOpExecutor = cpuIntensiveOpExecutor;
		this.completionIndicator = completionIndicator;
		this.exceptionHandler = exceptionHandler;
		this.cleanupHandler = cleanupHandler;
		this.responseProducer = responseProducer;
	}
}
