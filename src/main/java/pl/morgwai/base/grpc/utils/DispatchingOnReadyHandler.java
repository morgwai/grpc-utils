// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.CallStreamObserver;



/**
 * Streams messages to a {@link CallStreamObserver} from multiple threads with respect to
 * flow-control to ensure that no excessive buffering occurs. This class has similar purpose to
 * {@link io.grpc.stub.StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)}, but work
 * is dispatched to the supplied executor and parallelized according to the supplied param.
 * <p>
 * Typical usage:</p>
 * <pre>
 * public void myServerStreamingMethod(
 *         RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *     final var processor = new MyRequestProcessor(request, NUMBER_OF_TASKS);
 *     final var responseObserver =
 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *     responseObserver.setOnCancelHandler(() -&gt; log.fine("client cancelled"));
 *     responseObserver.setOnReadyHandler(new DispatchingOnReadyHandler&lt;&gt;(
 *         responseObserver,
 *         taskExecutor,
 *         NUMBER_OF_TASKS,
 *         (taskNumber) -&gt; processor.isCompleted(taskNumber),
 *         (taskNumber) -&gt; processor.produceNextResponseMessage(taskNumber),
 *         (taskNumber, error) -&gt; {
 *             processor.fail(error);  // interrupt other tasks
 *             if (error instanceof StatusRuntimeException) return Optional.empty();
 *             return Optional.of(Status.INTERNAL.asException());
 *         },
 *         (taskNumber) -&gt; processor.cleanup(i)
 *     ));
 * }</pre>
 */
public class DispatchingOnReadyHandler<ResponseT> implements Runnable {



	@FunctionalInterface
	public interface ThrowingFunction<ParamT, ResultT> {
		ResultT apply(ParamT param) throws Exception;
	}



	/**
	 * Constructs a multi-threaded handler that includes exception handling.
	 * If {@code exceptionHandler} applied to an exception thrown by {@code completionIndicator} or
	 * {@code messageProducer} returns non-empty result, then it will be passed on to
	 * {@code streamObserver.onError(...)}.
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		ThrowingFunction<Integer, Boolean> completionIndicator,
		ThrowingFunction<Integer, ResponseT> messageProducer,
		BiFunction<Integer, Throwable, Optional<Throwable>> exceptionHandler,
		Consumer<Integer> cleanupHandler
	) {
		this(streamObserver, taskExecutor, numberOfTasks);
		this.completionIndicator = completionIndicator;
		this.messageProducer = messageProducer;
		this.exceptionHandler = exceptionHandler;
		this.cleanupHandler = cleanupHandler;
	}



	/**
	 * Constructs a multi-threaded handler for "no-exception" case.
	 * <p>
	 * If {@link Error} or {@link RuntimeException} occurs, it will be passed on to
	 * {@code streamObserver.onError(...)} (except for {@link StatusRuntimeException}) and re-thrown
	 * (including {@link StatusRuntimeException}).</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		Function<Integer, Boolean> completionIndicator,
		Function<Integer, ResponseT> messageProducer
	) {
		this(streamObserver, taskExecutor, numberOfTasks);
		this.completionIndicator = completionIndicator::apply;
		this.messageProducer = messageProducer::apply;
		this.exceptionHandler = (taskNumber, excp) -> {
			if ( ! (excp instanceof StatusRuntimeException)) {
				synchronized (lock) {
					streamObserver.onError(excp);
				}
			}
			if (excp instanceof Error) throw (Error) excp;
			if (excp instanceof RuntimeException) throw (RuntimeException) excp;
			return Optional.empty();
		};
	}



	/**
	 * Similar to
	 * {@link #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, Function, Function)}
	 * but uses {@link Iterator} API instead of 2 {@link Function}s.
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		Function<Integer, Iterator<ResponseT>> messageProducer
	) {
		this(
			streamObserver,
			taskExecutor,
			numberOfTasks,
			(taskNumber) -> !messageProducer.apply(taskNumber).hasNext(),
			(taskNumber) -> messageProducer.apply(taskNumber).next()
		);
	}



	/**
	 * Constructs a single-threaded handler for "no-exception" case.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, Function,
	 * Function) multi-threaded version for details about exception handling
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
			(taskNumber) -> completionIndicator.get(),
			(taskNumber) -> messageProducer.get()
		);
	}



	/**
	 * Similar to
	 * {@link #DispatchingOnReadyHandler(CallStreamObserver, Executor, Supplier, Supplier)} but uses
	 * {@link Iterator} API instead of 2 {@link Supplier}s.
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		Iterator<ResponseT> messageProducer
	) {
		this(
			streamObserver,
			taskExecutor,
			1,
			(taskNumber) -> !messageProducer.hasNext(),
			(taskNumber) -> messageProducer.next()
		);
	}



	/**
	 * Constructs a single-thread handler that includes exception handling.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, ThrowingFunction,
	 * ThrowingFunction, BiFunction, Consumer) multi-threaded version for details about
	 * exception handling
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		Callable<Boolean> completionIndicator,
		Callable<ResponseT> messageProducer,
		Function<Throwable, Optional<Throwable>> exceptionHandler,
		Runnable cleanupHandler
	) {
		this(
			streamObserver,
			taskExecutor,
			1,
			(taskNumber) -> completionIndicator.call(),
			(taskNumber) -> messageProducer.call(),
			(taskNumber, excp) -> exceptionHandler.apply(excp),
			(taskNumber) -> cleanupHandler.run()
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
		taskRunning = new boolean[numberOfTasks];
	}

	CallStreamObserver<ResponseT> streamObserver;
	Executor taskExecutor;
	int numberOfTasks;
	boolean[] taskRunning;
	final Object lock = new Object();

	AtomicInteger completionCount = new AtomicInteger(0);
	boolean errorReported = false;



	/**
	 * Indicates if the task {@code i} is completed.
	 * Default implementation calls {@link #completionIndicator}.
	 */
	protected boolean isCompleted(int taskNumber) throws Exception {
		return completionIndicator.apply(taskNumber);
	}

	/**
	 * Called by {@link #isCompleted(int)}.
	 */
	protected ThrowingFunction<Integer, Boolean> completionIndicator;



	/**
	 * Asks task {@code i} to produce a next message.
	 * Default implementation calls {@link #messageProducer}.
	 */
	protected ResponseT produceMessage(int taskNumber) throws Exception {
		return messageProducer.apply(taskNumber);
	}

	/**
	 * Called by {@link #produceMessage(int)}.
	 */
	protected ThrowingFunction<Integer, ResponseT> messageProducer;



	/**
	 * Handles exception thrown by task {@code i}.
	 * Default implementation calls {@link #exceptionHandler}.
	 */
	protected Optional<Throwable> handleException(int taskNumber, Throwable excp) {
		return exceptionHandler.apply(taskNumber, excp);
	}

	/**
	 * Called by {@link #handleException(int, Throwable)}.
	 */
	protected BiFunction<Integer, Throwable, Optional<Throwable>> exceptionHandler;



	/**
	 * Cleans up after task {@code i} is completed.
	 * Default implementation calls {@link #cleanupHandler}.
	 */
	protected void cleanup(int taskNumber) {
		if (cleanupHandler != null) cleanupHandler.accept(taskNumber);
	}

	/**
	 * Called by {@link #cleanup(int)}.
	 */
	protected Consumer<Integer> cleanupHandler;



	/**
	 * Sets handler to obtain String representation of task {@code i} for logging purposes.
	 */
	public void setTaskToStringHandler(Function<Integer, String> taskToStringHandler) {
		this.taskToStringHandler = taskToStringHandler;
	}
	Function<Integer, String> taskToStringHandler;



	/**
	 * Dispatches tasks to handle a single cycle of observer's readiness.
	 */
	public void run() {
		synchronized (lock) {
			if (errorReported) return;
			for (int taskNumber = 0; taskNumber < numberOfTasks; taskNumber++) {
				// it may happen that responseObserver will change its state from unready to ready
				// very fast, before some tasks can even notice. Such tasks will span over more than
				// 1 cycle and taskRunning flags prevent dispatching redundant tasks in in such case
				if (taskRunning[taskNumber]) continue;
				taskRunning[taskNumber] = true;
				taskExecutor.execute(new SingleReadinessCycleHandlerTask(taskNumber));
			}
		}
	}



	class SingleReadinessCycleHandlerTask implements Runnable {

		final int taskNumber;

		SingleReadinessCycleHandlerTask(int taskNumber) { this.taskNumber = taskNumber; }



		@Override public void run() {
			var ready = true;
			try {
				if ( !isCompleted(taskNumber)) {
					synchronized (lock) {
						ready = streamObserver.isReady();
						if ( !ready) {
							taskRunning[taskNumber] = false;
							return;
						}
					}
					do {
						final var responseMessage = produceMessage(taskNumber);
						final var completed = isCompleted(taskNumber);
						synchronized (lock) {
							streamObserver.onNext(responseMessage);
							if (completed) break; // no need to check isReady, break immediately
							ready = streamObserver.isReady();
							if ( !ready) {
								taskRunning[taskNumber] = false;
								return;
							}
						}
					} while (true); // completed/unready cause break/return, no need for extra check
				}
				if (completionCount.incrementAndGet() == numberOfTasks) {
					synchronized (lock) {
						streamObserver.onCompleted();
					}
				}
				// taskRunning[taskNumber] is left true to not respawn completed tasks
			} catch (Throwable throwable) {
				final var handlingResult = handleException(taskNumber, throwable);
				if (handlingResult.isEmpty()) return;
				final var toReport = handlingResult.get();
				synchronized (lock) {
					if ( !errorReported) {
						streamObserver.onError(toReport);
						errorReported = true;
					}
				}
			} finally {
				if (ready) cleanup(taskNumber);  // only call on exception or completion
			}
		}



		@Override public String toString() {
			return taskToStringHandler != null
					? taskToStringHandler.apply(taskNumber)
					: "onReadyHandler-task-" + taskNumber;
		}
	}
}
