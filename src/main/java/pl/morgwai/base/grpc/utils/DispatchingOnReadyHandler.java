// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;



/**
 * Handles message streaming to observer, while ensuring that no excessive response buffering
 * happens, in cases when message producing must be dispatched to an external executor.
 * Setting an instance as an <code>onReadyHandler</code> will eventually have similar effects as if
 * the below code was dispatched to {@link #processingExecutor}:
 * <pre>
 *try {
 *    while ( ! completionIndicator.call())
 *        streamObserver.onNext(messageProducer.call());
 *    streamObserver.onCompleted();
 *} catch (Throwable t) {
 *    exceptionHandler.accept(t);
 *} finally {
 *    cleanupHandler.run();
 *}
 * </pre>
 * However, the work is automatically suspended/resumed whenever {@link #streamObserver} becomes
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
 *            if ( ! (t instanceof StatusRuntimeException)) responseObserver.onError(t);
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
 *public void myServerStreamingMethod(
 *        RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *    responseObserver.setOnReadyHandler(() -&gt; {
 *        synchronized (responseObserver) {
 *            responseObserver.notify();
 *        }
 *    });
 *    jdbcExecutor.execute(() -&gt; {
 *        try {
 *            var state = new MyCallState(request);
 *            while ( ! state.isCompleted()) {
 *                synchronized (responseObserver) {
 *                    while ( ! responseObserver.isReady()) responseObserver.wait();
 *                }
 *                responseObserver.onNext(state.produceNextResponseMessage());
 *            }
 *            responseObserver.onCompleted();
 *        } catch (Throwable t) {
 *            if ( ! (t instanceof StatusRuntimeException)) responseObserver.onError(t);
 *        } finally {
 *            state.cleanup();
 *        }
 *    });
 *}
 * </pre>
 */
public class DispatchingOnReadyHandler<ResponseT> implements Runnable {



	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor processingExecutor,
		Callable<Boolean> completionIndicator,
		Callable<ResponseT> messageProducer,
		Consumer<Throwable> exceptionHandler,
		Runnable cleanupHandler
	) {
		this(streamObserver, processingExecutor);
		this.completionIndicator = completionIndicator;
		this.messageProducer = messageProducer;
		this.exceptionHandler = exceptionHandler;
		this.cleanupHandler = cleanupHandler;
	}

	/**
	 * Constructor for those who prefer to override {@link #isCompleted()},
	 * {@link #produceMessage()}, {@link #handleException(Throwable)} and {@link #cleanup()} in a
	 * subclass instead of providing lambdas.
	 */
	protected DispatchingOnReadyHandler(
			CallStreamObserver<ResponseT> streamObserver,
			Executor processingExecutor
	) {
		this.streamObserver = streamObserver;
		this.processingExecutor = processingExecutor;
	}

	CallStreamObserver<ResponseT> streamObserver;
	Executor processingExecutor;



	protected boolean isCompleted() throws Exception {
		return completionIndicator.call();
	}

	Callable<Boolean> completionIndicator;



	protected ResponseT produceMessage() throws Exception {
		return messageProducer.call();
	}

	Callable<ResponseT> messageProducer;



	protected void handleException(Throwable error) {
		exceptionHandler.accept(error);
	}

	Consumer<Throwable> exceptionHandler;



	protected void cleanup() {
		cleanupHandler.run();
	}

	Runnable cleanupHandler;



	public synchronized void run() {
		if (processingInProgress) return;
		processingInProgress = true;
		processingExecutor.execute(() -> handleSingleReadinessCycle());
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
	void handleSingleReadinessCycle() {
		var ready = isReady();
		try {
			while (ready && ! isCompleted()) {
				streamObserver.onNext(produceMessage());
				ready = isReady();
			}
			if (ready) streamObserver.onCompleted();
		} catch (Throwable t) {
			handleException(t);;
		} finally {
			if (ready) cleanup();
		}
	}

	synchronized boolean isReady() {
		if (streamObserver.isReady()) return true;
		processingInProgress = false;
		return false;
	}
}
