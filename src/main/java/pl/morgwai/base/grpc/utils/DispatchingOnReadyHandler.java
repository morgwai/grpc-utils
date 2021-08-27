// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;



/**
 * Handles streaming of messages to a {@link CallStreamObserver} from multiple threads with respect
 * to flow-control to ensure that no excessive buffering occurs.
 * <p>
 * Setting an instance using {@link CallStreamObserver#setOnReadyHandler(Runnable)
 * setOnReadyHandler(dispatchingOnReadyHandler)} will eventually have similar effects as the below
 * pseudo-code:
 * <pre>
 *for (int i = 0; i &lt; numberOfTasks; i++) taskExecutor.execute(() -&gt; {
 *    try {
 *        while ( ! completionIndicator.apply(i))
 *            streamObserver.onNext(messageProducer.apply(i));
 *        if (allTasksCompleted()) streamObserver.onCompleted();
 *    } catch (Throwable t) {
 *        exceptionHandler.accept(i, t);
 *    } finally {
 *        cleanupHandler.accept(i);
 *    }
 *});
 * </pre>
 * However, calls to {@code streamObserver} are properly synchronized and the work is automatically
 * suspended/resumed whenever {@link #streamObserver} becomes unready/ready and executor's threads
 * are <b>released</b> during time when observer is unready.</p>
 * <p>
 * Typical usage:
 * <pre>
 *public void myServerStreamingMethod(
 *        RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *    var state = new MyCallState(request, NUMBER_OF_TASKS);
 *    var responseObserver =
 *            (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *    responseObserver.setOnCancelHandler(() -&gt; log.fine("client cancelled"));
 *    final var handler = new DispatchingServerStreamingCallHandler&lt;&gt;(
 *        responseObserver,
 *        taskExecutor,
 *        NUMBER_OF_TASKS,
 *        (i) -&gt; state.isCompleted(i),
 *        (i) -&gt; state.produceNextResponseMessage(i),
 *        (i, error) -&gt; {
 *            state.fail(error);  // interrupt other tasks
 *            if ( ! (error instanceof StatusRuntimeException)) {
 *                synchronized (handler) {
 *                    responseObserver.onError(error);
 *                }
 *            }
 *        },
 *        (i) -&gt; state.cleanup(i)
 *    );
 *    responseObserver.setOnReadyHandler(handler);
 *}
 * </pre></p>
 * <p>
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
 * </pre></p>
 */
public class DispatchingOnReadyHandler<ResponseT> implements Runnable {



	@FunctionalInterface
	public static interface ThrowingFunction<ParamT, ResultT> {
		ResultT apply(ParamT param) throws Exception;
	}



	/**
	 * Constructs a "full-version" handler that includes handling exception thrown by
	 * {@link #completionIndicator} and {@link #messageProducer}.
	 * <p>
	 * If {@link #exceptionHandler} does not interrupt all tasks itself and calls
	 * {@link StreamObserver#onError(Throwable) streamObserver.onError(error)}, then it must do so
	 * while synchronized on this {@code DispatchingOnReadyHandler} instance to avoid collisions
	 * with other tasks calling {@link StreamObserver#onNext(Object)}.</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		ThrowingFunction<Integer, Boolean> completionIndicator,
		ThrowingFunction<Integer, ResponseT> messageProducer,
		BiConsumer<Integer, Throwable> exceptionHandler,
		Consumer<Integer> cleanupHandler
	) {
		this(streamObserver, taskExecutor, numberOfTasks);
		this.completionIndicator = completionIndicator;
		this.messageProducer = messageProducer;
		this.exceptionHandler = exceptionHandler;
		this.cleanupHandler = cleanupHandler;
	}



	/**
	 * Constructs a handler for "no-exception" case.
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		Function<Integer, Boolean> completionIndicator,
		Function<Integer, ResponseT> messageProducer
	) {
		this(streamObserver, taskExecutor, numberOfTasks);
		this.completionIndicator = (i) -> completionIndicator.apply(i);
		this.messageProducer = (i) -> messageProducer.apply(i);
		this.exceptionHandler = (i, e) -> {
			if (e instanceof Error) throw (Error) e;
			if (e instanceof RuntimeException) throw (RuntimeException) e;
		};
	}



	/**
	 * Constructs a handler for "no-exception single-thread" case.
	 * <p>
	 * This is roughly equivalent to
	 * {@link io.grpc.stub.StreamObservers
	 * #copyWithFlowControl(java.util.Iterator, CallStreamObserver)}, except that it will run on
	 * different executor.</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		Supplier<Boolean> completionIndicator,
		Supplier<ResponseT> messageProducer
	) {
		this(
			streamObserver,
			taskExecutor,
			1,
			(i) -> completionIndicator.get(),
			(i) -> messageProducer.get()
		);
	}



	/**
	 * Constructs a handler for "single-thread" case that includes handling exception thrown by
	 * {@link #completionIndicator} and {@link #messageProducer}.
	 * <p>
	 * Unlike in
	 * {@link #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, ThrowingFunction,
	 * ThrowingFunction, BiConsumer, Consumer) the multithreaded version}, calls to
	 * {@link StreamObserver#onError(Throwable) streamObserver.onError(error)} in
	 * {@link #exceptionHandler} don't need to be synchronized, but it's still recommended to avoid
	 * mistakes.</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		Callable<Boolean> completionIndicator,
		Callable<ResponseT> messageProducer,
		Consumer<Throwable> exceptionHandler,
		Runnable cleanupHandler
	) {
		this(
			streamObserver,
			taskExecutor,
			1,
			(i) -> completionIndicator.call(),
			(i) -> messageProducer.call(),
			(i, error) -> exceptionHandler.accept(error),
			(i) -> cleanupHandler.run()
		);
	}



	/**
	 * Constructor for those who prefer to override {@link #isCompleted(int)},
	 * {@link #produceMessage(int)}, {@link #handleException(int, Throwable)} and
	 * {@link #cleanup(int)} in a subclass instead of providing lambdas.
	 */
	protected DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks
	) {
		this.streamObserver = streamObserver;
		this.taskExecutor = taskExecutor;
		this.numberOfTasks = numberOfTasks;
		completionCount = new AtomicInteger(0);
		taskRunning = new boolean[numberOfTasks];
	}

	CallStreamObserver<ResponseT> streamObserver;
	Executor taskExecutor;
	int numberOfTasks;
	AtomicInteger completionCount;
	boolean[] taskRunning;



	protected boolean isCompleted(int i) throws Exception {
		return completionIndicator.apply(i);
	}

	protected ThrowingFunction<Integer, Boolean> completionIndicator;



	protected ResponseT produceMessage(int i) throws Exception {
		return messageProducer.apply(i);
	}

	protected ThrowingFunction<Integer, ResponseT> messageProducer;



	protected void handleException(int i, Throwable error) {
		if (exceptionHandler != null) exceptionHandler.accept(i, error);
	}

	protected BiConsumer<Integer, Throwable> exceptionHandler;



	protected void cleanup(int i) {
		if (cleanupHandler != null) cleanupHandler.accept(i);
	}

	protected Consumer<Integer> cleanupHandler;



	/**
	 * Dispatches tasks to handle a single cycle of {@link #responseObserver}'s readiness.
	 */
	public synchronized void run() {
		for (int i = 0; i < numberOfTasks; i++) {
			// it may happen that responseObserver will change its state from unready to ready very
			// fast, before some tasks can even notice. Such tasks will span over more than 1 cycle
			// and taskRunning flags prevent dispatching redundant tasks in in such case.
			if (taskRunning[i]) continue;
			taskRunning[i] = true;
			final var taskNumber = Integer.valueOf(i);
			taskExecutor.execute(() -> handleSingleReadinessCycle(taskNumber));
		}
	}



	void handleSingleReadinessCycle(Integer taskNumber) {
		var ready = true;
		try {
			synchronized (this) {
				ready = streamObserver.isReady();
				if ( ! ready) taskRunning[taskNumber] = false;;
			}
			while (ready && ! isCompleted(taskNumber)) {
				final var responseMessage = produceMessage(taskNumber);
				synchronized (this) {
					streamObserver.onNext(responseMessage);
					ready = streamObserver.isReady();
					if ( ! ready) taskRunning[taskNumber] = false;;
				}
			}
			if (ready && completionCount.incrementAndGet() == numberOfTasks) {
				streamObserver.onCompleted();
			}
		} catch (Throwable throwable) {
			handleException(taskNumber, throwable);
		} finally {
			if (ready) cleanup(taskNumber);
		}
	}
}
