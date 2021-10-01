// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;



/**
 * Handles streaming of messages to a {@link CallStreamObserver} from multiple threads with respect
 * to flow-control to ensure that no excessive buffering occurs.
 * <p>
 * Setting an instance using {@link CallStreamObserver#setOnReadyHandler(Runnable)
 * setOnReadyHandler(dispatchingOnReadyHandler)} will eventually have similar effects as the below
 * pseudo-code:</p>
 * <pre>
 * for (int i = 0; i &lt; numberOfTasks; i++) taskExecutor.execute(() -&gt; {
 *     try {
 *         while ( ! completionIndicator.apply(i))
 *             streamObserver.onNext(messageProducer.apply(i));
 *         if (allTasksCompleted()) streamObserver.onCompleted();
 *     } catch (Throwable t) {
 *         var toReport = handleException(taskNumber, throwable);
 *         if (toReport != null) streamObserver.onError(toReport);
 *     } finally {
 *         cleanupHandler.accept(i);
 *     }
 * });</pre>
 * <p>
 * However, calls to {@code streamObserver} are properly synchronized and the work is automatically
 * suspended/resumed whenever {@link #streamObserver} becomes unready/ready and executor's threads
 * are <b>released</b> during time when observer is unready.</p>
 * <p>
 * Typical usage:</p>
 * <pre>
 * public void myServerStreamingMethod(
 *         RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *     var state = new MyCallState(request, NUMBER_OF_TASKS);
 *     var responseObserver =
 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *     responseObserver.setOnCancelHandler(() -&gt; log.fine("client cancelled"));
 *     final var handler = new DispatchingServerStreamingCallHandler&lt;&gt;(
 *         responseObserver,
 *         taskExecutor,
 *         NUMBER_OF_TASKS,
 *         (i) -&gt; state.isCompleted(i),
 *         (i) -&gt; state.produceNextResponseMessage(i),
 *         (i, error) -&gt; {
 *             state.fail(error);  // interrupt other tasks
 *             if (error instanceof StatusRuntimeException) return null;
 *             return Status.INTERNAL.asException();
 *         },
 *         (i) -&gt; state.cleanup(i)
 *     );
 *     responseObserver.setOnReadyHandler(handler);
 * }</pre>
 * <p>
 * <b>NOTE:</b> this class is not suitable for cases where executor's thread should not be released,
 * such as JDBC/JPA processing where executor threads correspond to pooled connections that must be
 * retained in order not to lose given DB transaction/cursor. In such cases processing should be
 * implemented similar as the below code:</p>
 * <pre>
 * public void myServerStreamingMethod(
 *         RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *     responseObserver.setOnReadyHandler(() -&gt; {
 *         synchronized (responseObserver) {
 *             responseObserver.notify();
 *         }
 *     });
 *     jdbcExecutor.execute(() -&gt; {
 *         try {
 *             var state = new MyCallState(request);
 *             while ( ! state.isCompleted()) {
 *                 synchronized (responseObserver) {
 *                     while ( ! responseObserver.isReady()) responseObserver.wait();
 *                 }
 *                 responseObserver.onNext(state.produceNextResponseMessage());
 *             }
 *             responseObserver.onCompleted();
 *         } catch (Throwable t) {
 *             if ( ! (t instanceof StatusRuntimeException)) responseObserver.onError(t);
 *         } finally {
 *             state.cleanup();
 *         }
 *     });
 * }</pre>
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
	 * If and only if {@link #exceptionHandler} returns non-null and
	 * {@link StreamObserver#onError(Throwable)} hasn't been called yet, then it will be called with
	 * obtained value as its argument.</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		int numberOfTasks,
		ThrowingFunction<Integer, Boolean> completionIndicator,
		ThrowingFunction<Integer, ResponseT> messageProducer,
		BiFunction<Integer, Throwable, Throwable> exceptionHandler,
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
	 * <p>
	 * If {@link Error} or {@link RuntimeException} occurs, it is reported via
	 * {@link StreamObserver#onError(Throwable)} (except for {@link StatusRuntimeException} and
	 * unless some error has been already reported) and re-thrown (including
	 * {@link StatusRuntimeException}).</p>
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
		this.exceptionHandler = (i, e) -> {
			if ( ! (e instanceof StatusRuntimeException)) {
				synchronized (lock) {
					streamObserver.onError(e);
				}
			}
			if (e instanceof Error) throw (Error) e;
			if (e instanceof RuntimeException) throw (RuntimeException) e;
			return null;
		};
	}



	/**
	 * Constructs a handler for "no-exception single-thread" case.
	 * <p>
	 * This is roughly equivalent to
	 * {@link io.grpc.stub.StreamObservers
	 * #copyWithFlowControl(java.util.Iterator, CallStreamObserver)}, except that it will run on
	 * different executor and for {@link Error}/{@link RuntimeException} reporting.</p>
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
	 * @see DispatchingOnReadyHandler
	 * #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, ThrowingFunction,
	 * ThrowingFunction, BiFunction, Consumer) multi-task constructor for details about exception
	 * handling
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResponseT> streamObserver,
		Executor taskExecutor,
		Callable<Boolean> completionIndicator,
		Callable<ResponseT> messageProducer,
		Function<Throwable, Throwable> exceptionHandler,
		Runnable cleanupHandler
	) {
		this(
			streamObserver,
			taskExecutor,
			1,
			(i) -> completionIndicator.call(),
			(i) -> messageProducer.call(),
			(i, error) -> exceptionHandler.apply(error),
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
		taskRunning = new boolean[numberOfTasks];
	}

	CallStreamObserver<ResponseT> streamObserver;
	Executor taskExecutor;
	int numberOfTasks;
	boolean[] taskRunning;
	Object lock = new Object();

	AtomicInteger completionCount = new AtomicInteger(0);
	boolean errorReported = false;



	/**
	 * Indicates if the task {@code i} is completed.
	 * Default implementation calls {@link #completionIndicator}.
	 */
	protected boolean isCompleted(int i) throws Exception {
		return completionIndicator.apply(i);
	}

	/**
	 * Called by {@link #isCompleted(int)}.
	 */
	protected ThrowingFunction<Integer, Boolean> completionIndicator;



	/**
	 * Asks task {@code i} to produce a next message.
	 * Default implementation calls {@link #messageProducer}.
	 */
	protected ResponseT produceMessage(int i) throws Exception {
		return messageProducer.apply(i);
	}

	/**
	 * Called by {@link #produceMessage(int)}.
	 */
	protected ThrowingFunction<Integer, ResponseT> messageProducer;



	/**
	 * Handles exception thrown by task {@code i}.
	 * Default implementation calls {@link #exceptionHandler}.
	 */
	protected Throwable handleException(int i, Throwable error) {
		return exceptionHandler.apply(i, error);
	}

	/**
	 * Called by {@link #handleException(int, Throwable)}.
	 */
	protected BiFunction<Integer, Throwable, Throwable> exceptionHandler;



	/**
	 * Cleans up after task {@code i} is completed.
	 * Default implementation calls {@link #cleanupHandler}.
	 */
	protected void cleanup(int i) {
		if (cleanupHandler != null) cleanupHandler.accept(i);
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
			for (int i = 0; i < numberOfTasks; i++) {
				// it may happen that responseObserver will change its state from unready to ready
				// very fast, before some tasks can even notice. Such tasks will span over more than
				// 1 cycle and taskRunning flags prevent dispatching redundant tasks in in such case
				if (taskRunning[i]) continue;
				taskRunning[i] = true;
				final var taskNumber = Integer.valueOf(i);
				taskExecutor.execute(new Runnable() {

					@Override public void run() { handleSingleReadinessCycle(taskNumber); }

					@Override public String toString() {
						return taskToStringHandler != null
								? taskToStringHandler.apply(taskNumber)
								: "dispatchedOnReadyHandler-task-" + taskNumber;
					}
				});
			}
		}
	}



	void handleSingleReadinessCycle(Integer taskNumber) {
		var ready = true;
		try {
			if ( ! isCompleted(taskNumber)) {
				synchronized (lock) {
					ready = streamObserver.isReady();
					if ( ! ready) {
						taskRunning[taskNumber] = false;
						return;
					}
				}
				do {
					final var responseMessage = produceMessage(taskNumber);
					var completed = isCompleted(taskNumber);
					synchronized (lock) {
						streamObserver.onNext(responseMessage);
						if (completed) break;  // don't check isReady, call onCompleted immediately
						ready = streamObserver.isReady();
						if ( ! ready) {
							taskRunning[taskNumber] = false;
							return;
						}
					}
				} while (true);  // completed/unready cause break/return: no need for extra check
			}
			if (completionCount.incrementAndGet() == numberOfTasks) {
				synchronized (lock) {
					streamObserver.onCompleted();
				}
			}
			// taskRunning[taskNumber] is left true to not re-spawn completed tasks unnecessarily
		} catch (Throwable throwable) {
			var toReport = handleException(taskNumber, throwable);
			if (toReport != null) {
				synchronized (lock) {
					if ( ! errorReported) {
						streamObserver.onError(toReport);
						errorReported = true;
					}
				}
			}
		} finally {
			if (ready) cleanup(taskNumber);  // only call on exception or completion
		}
	}
}
