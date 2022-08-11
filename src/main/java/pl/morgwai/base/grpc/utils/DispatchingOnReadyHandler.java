// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.*;

import io.grpc.*;
import io.grpc.stub.CallStreamObserver;



/**
 * Streams messages to a {@link CallStreamObserver} from multiple threads with respect to
 * flow-control to ensure that no excessive buffering occurs. This class has similar purpose to
 * {@link io.grpc.stub.StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)}, but work
 * is dispatched to the supplied executor and parallelized according to the supplied param.
 * <p>
 * Typical usage in streaming-server methods:</p>
 * <pre>
 * public void myServerStreamingMethod(
 *         RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *     final var processor = new MyRequestProcessor(request, NUMBER_OF_TASKS);
 *     final var responseObserver =
 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *     responseObserver.setOnCancelHandler(() -&gt; processor.cancel());
 *     DispatchingOnReadyHandler.copyWithFlowControl(
 *         responseObserver,
 *         taskExecutor,
 *         NUMBER_OF_TASKS,
 *         (taskNumber) -&gt; processor.hasMoreResults(taskNumber),
 *         (taskNumber) -&gt; processor.produceNextResult(taskNumber)
 *     ));
 * }</pre>
 */
public class DispatchingOnReadyHandler<ResultT> implements Runnable {



	@SafeVarargs
	public DispatchingOnReadyHandler(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		Iterator<ResultT>... taskProducers
	) {
		this.taskProducers = taskProducers;
		this.outboundObserver = outboundObserver;
		this.taskExecutor = taskExecutor;
		this.waitForOtherTasksToFinishOnException = waitForOtherTasksToFinishOnException;
		taskRunning = new boolean[taskProducers.length];
	}

	@SafeVarargs
	public static <ResultT> void copyWithFlowControl(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		Iterator<ResultT>... producers
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			producers
		));
	}

	@SafeVarargs
	public static <ResultT> void copyWithFlowControl(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		Iterator<ResultT>... producers
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			false,
			producers
		));
	}

	public static <ResultT> void copyWithFlowControl(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		Supplier<Boolean> hasMoreResultsIndicator,
		Supplier<ResultT> resultProducer
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			false,
			new Iterator<>() {
				@Override public boolean hasNext() { return hasMoreResultsIndicator.get(); }
				@Override public ResultT next() { return resultProducer.get(); }
				@Override public String toString() { return "single"; }
			}
		));
	}



	public DispatchingOnReadyHandler(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		int numberOfTasks,
		Function<Integer, Boolean> taskHasMoreResultsIndicator,
		Function<Integer, ResultT> taskResultProducer,
		Function<Integer, String> taskToString
	) {
		this(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			producerFunctionsToIterators(
					numberOfTasks, taskHasMoreResultsIndicator, taskResultProducer, taskToString)
		);
	}

	public static <ResultT> void copyWithFlowControl(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		int numberOfTasks,
		Function<Integer, Boolean> taskHasMoreResultsIndicator,
		Function<Integer, ResultT> taskResultProducer
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			numberOfTasks,
			taskHasMoreResultsIndicator,
			taskResultProducer,
			Object::toString
		));
	}

	public static <ResultT> void copyWithFlowControl(
		CallStreamObserver<ResultT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		Function<Integer, Boolean> taskHasMoreResultsIndicator,
		Function<Integer, ResultT> taskResultProducer
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			false,
			numberOfTasks,
			taskHasMoreResultsIndicator,
			taskResultProducer,
			Object::toString
		));
	}



	final CallStreamObserver<ResultT> outboundObserver;
	final Executor taskExecutor;
	final boolean waitForOtherTasksToFinishOnException;
	final Iterator<ResultT>[] taskProducers;

	final boolean[] taskRunning;
	final Object lock = new Object();
	int completedTaskCount = 0;
	Throwable error;



	/**
	 * For each task dispatches to {@code taskExecutor} a handler of a single cycle of readiness of
	 * {@code outboundObserver}.
	 */
	public void run() {
		synchronized (lock) {
			if (completedTaskCount == taskProducers.length
					|| (error != null && !waitForOtherTasksToFinishOnException)) {
				return;
			}
			for (int taskNumber = 0; taskNumber < taskProducers.length; taskNumber++) {
				// it may happen that responseObserver will change its state from unready to ready
				// very fast, before some tasks can even notice. Such tasks will span over more than
				// 1 cycle and taskRunning flags prevent dispatching redundant tasks in in such case
				if (taskRunning[taskNumber]) continue;
				taskRunning[taskNumber] = true;
				taskExecutor.execute(new TaskOnReadyHandler(taskNumber));
			}
		}
	}



	/**
	 * Handles a single cycle of readiness for a task with a number given by the constructor param.
	 */
	class TaskOnReadyHandler implements Runnable {

		final int taskNumber;
		final Iterator<ResultT> taskProducer;



		TaskOnReadyHandler(int taskNumber) {
			this.taskNumber = taskNumber;
			taskProducer = taskProducers[taskNumber];
		}



		@Override public void run() {
			try {
				boolean ready;
				synchronized (lock) {
					ready = outboundObserver.isReady();
				}
				while (ready && taskProducer.hasNext()) {
					final var result = taskProducer.next();
					synchronized (lock) {
						outboundObserver.onNext(result);
						ready = outboundObserver.isReady();
					}
				}
				if ( !ready) {
					taskRunning[taskNumber] = false;
				} else synchronized (lock) {
					if (++completedTaskCount < taskProducers.length) return;
					// taskRunning[taskNumber] is left true to not respawn completed tasks
					if (error == null) {
						outboundObserver.onCompleted();
					} else {
						outboundObserver.onError(error);
					}
				}
			} catch (Throwable throwable) {
				synchronized (lock) {
					if (error == null) {
						error = throwable instanceof StatusRuntimeException
								? throwable
								: Status.INTERNAL.withCause(throwable).asException();
					}
					if (!waitForOtherTasksToFinishOnException
							|| ++completedTaskCount == taskProducers.length) {
						outboundObserver.onError(error);
					}
				}
				throw throwable;
			}
		}



		@Override public String toString() {
			return "onReadyHandlerTask-" + taskProducer;
		}
	}



	public static <T> Iterator<T>[] producerFunctionsToIterators(
		int numberOfTasks,
		Function<Integer, Boolean> taskHasMoreResultsIndicator,
		Function<Integer, T> taskResultProducer,
		Function<Integer, String> taskToString
	) {
		@SuppressWarnings("unchecked")
		Iterator<T>[] producers = new Iterator[numberOfTasks];
		for (var i = 0; i < numberOfTasks; i++) {
			final var taskNumber = i;
			producers[taskNumber] = new Iterator<>() {

				@Override public boolean hasNext() {
					return !taskHasMoreResultsIndicator.apply(taskNumber);
				}

				@Override public T next() {
					return taskResultProducer.apply(taskNumber);
				}

				@Override public String toString() {
					return taskToString.apply(taskNumber);
				}
			};
		}
		return producers;
	}
}
