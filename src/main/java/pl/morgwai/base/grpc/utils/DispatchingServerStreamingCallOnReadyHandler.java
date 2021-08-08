// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Function;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;



/**
 * Handles server streaming calls that need to dispatch response producing to an external executor
 * while ensuring that no excessive response buffering happens.
 * Setting an instance as an <code>onReadyHandler</code> will eventually have similar effects as if
 * the below code was dispatched to {@link #processingExecutor}:
 * <pre>
 *try {
 *    while ( ! completionIndicator.call())
 *        responseObserver.onNext(responseProducer.call());
 *    responseObserver.onCompleted();
 *} catch (Throwable t) {
 *    exceptionHandler.apply(t);
 *} finally {
 *    cleanupHandler.run();
 *}
 * </pre>
 * However, the work is automatically suspended/resumed whenever {@link #responseObserver} becomes
 * unready/ready and executor's thread is <b>released</b> whenever observer becomes unready.<br/>
 * <br/>
 * Typical usage:
 * <pre>
 *public void myServerStreamingMethod(
 *        RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *    var state = new MyCallState(request);
 *    var responseObserver =
 *            (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *    responseObserver.setOnCancelHandler(() -&gt; log.fine("client cancelled"));
 *    responseObserver.setOnReadyHandler(new DispatchingServerStreamingCallHandler&lt;&gt;(
 *        responseObserver,
 *        processingExecutor,
 *        () -&gt; state.isCompleted(),
 *        () -&gt; state.produceNextResponseMessage(),
 *        (Throwable t) -&gt; {
 *            log.log(Level.SEVERE, "exception", t);
 *            return sendAndRethrowErrorIfNeeded(t, responseObserver);
 *        },
 *        () -&gt; state.cleanup()
 *    ));
 *}
 * </pre>
 * <br/>
 * <b>NOTE:</b> this class is not suitable for cases where executor's thread should not be released,
 * such as JDBC/JPA processing where executor threads correspond to pooled connections that must be
 * retained in order not to lose given DB transaction/cursor. In such cases processing should be
 * implemented similar as the below code:
 * <pre>
 *responseObserver.setOnReadyHandler(() -&gt; {
 *    synchronized (responseObserver) {
 *        responseObserver.notify();
 *    }
 *});
 *jdbcExecutor.execute(() -&gt; {
 *    try {
 *        var state = new MyCallState(request);
 *        while ( ! state.isCompleted()) {
 *            synchronized (responseObserver) {
 *                while ( ! responseObserver.isReady()) responseObserver.wait();
 *            }
 *            responseObserver.onNext(state.produceNextResponseMessage());
 *        }
 *        responseObserver.onCompleted();
 *    } catch (Throwable t) {
 *        log.log(Level.SEVERE, "exception", t);
 *        sendAndRethrowErrorIfNeeded(t, responseObserver);
 *    } finally {
 *        state.cleanup();
 *    }
 *});
 * </pre>
 */
public class DispatchingServerStreamingCallOnReadyHandler<ResponseT> implements Runnable {



	ServerCallStreamObserver<ResponseT> responseObserver;
	Executor processingExecutor;
	Callable<Boolean> completionIndicator;
	Callable<ResponseT> responseProducer;
	Function<Throwable, Void> exceptionHandler;
	Runnable cleanupHandler;



	public DispatchingServerStreamingCallOnReadyHandler(
		ServerCallStreamObserver<ResponseT> responseObserver,
		Executor processingExecutor,
		Callable<Boolean> completionIndicator,
		Callable<ResponseT> responseProducer,
		Function<Throwable, Void> exceptionHandler,
		Runnable cleanupHandler
	) {
		this.responseObserver = responseObserver;
		this.processingExecutor = processingExecutor;
		this.completionIndicator = completionIndicator;
		this.responseProducer = responseProducer;
		this.exceptionHandler = exceptionHandler;
		this.cleanupHandler = cleanupHandler;
	}



	/**
	 * Common processing of exceptions for {@link #exceptionHandler}: calls
	 * {@link StreamObserver#onError(Throwable)} if <code>t</code> is <b>not</b> a
	 * {@link StatusRuntimeException} and re-throws <code>t</code> if it is an {@link Error}.
	 */
	public static Void sendAndRethrowErrorIfNeeded(
			Throwable t, StreamObserver<?> responseObserver) {
		if ( ! (t instanceof StatusRuntimeException)) {
			responseObserver.onError(Status.INTERNAL.withCause(t).asException());
		}
		if (t instanceof Error) throw (Error) t;
		return null;
	}



	public void run() {
		synchronized (responseObserver) {
			if (processingInProgress) return;
			processingInProgress = true;
		}
		processingExecutor.execute(singleReadinessCycleHandler);
	}

	boolean processingInProgress = false;

	/**
	 * Handles 1 cycle of {@link #responseObserver}'s readiness.<br/>
	 * <br/>
	 * Note: it may actually happen that {@link #responseObserver} will change its state from
	 * unready to ready very fast, before the handler can even notice. In such case the handler will
	 * span over more than 1 cycle. {@link #processingInProgress} flag prevents {@link #handle()}
	 * spawning another handler in such case.
	 */
	Runnable singleReadinessCycleHandler = () -> {
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
		} catch (Throwable t) {
			exceptionHandler.apply(t);
		} finally {
			cleanupHandler.run();
		}
	};
}
