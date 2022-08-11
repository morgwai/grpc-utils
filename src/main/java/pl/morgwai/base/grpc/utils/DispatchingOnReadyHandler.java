// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.*;

import io.grpc.*;
import io.grpc.stub.CallStreamObserver;



/**
 * Streams messages to an outbound {@link CallStreamObserver} from multiple sources in separate
 * threads with respect to flow-control. Useful in sever methods when 1 request message can result
 * in multiple response messages that can be produced concurrently in separate tasks.
 * This class has similar purpose to
 * {@link io.grpc.stub.StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)}, but work
 * is dispatched to the supplied executor and parallelized according to the supplied arguments.
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
public class DispatchingOnReadyHandler<MessageT> implements Runnable {



	/**
	 * Constructs a new handler with a number of tasks based on the length of {@code taskProducers}.
	 * If any task throws an unexpected exception, it will be passed to
	 * {@link Status#withCause(Throwable)} of {@link Status#INTERNAL} and reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(status.asException())}.
	 * @param outboundObserver target outbound observer to which messages from
	 *     {@code messageProducers} will be streamed.
	 * @param taskExecutor executor to which message producing tasks will be dispatched.
	 * @param waitForOtherTasksToFinishOnException whether the remaining tasks should be allowed
	 *     to complete in case some task throws an unexpected exception. After the remaining tasks
	 *     are completed, the thrown exception will be passed to {@link Status#INTERNAL} as
	 *     explained above. If multiple tasks throw, the first exception will be passed and the rest
	 *     will be discarded.
	 * @param messageProducers each element produces in a separate task (dispatched to
	 *     {@code taskExecutor}), messages that will be streamed to {@code outboundObserver}.
	 */
	@SafeVarargs
	public DispatchingOnReadyHandler(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		Iterator<MessageT>... messageProducers
	) {
		this.messageProducers = messageProducers;
		this.outboundObserver = outboundObserver;
		this.taskExecutor = taskExecutor;
		this.waitForOtherTasksToFinishOnException = waitForOtherTasksToFinishOnException;
		taskRunning = new boolean[messageProducers.length];
	}

	/**
	 * Convenience function that constructs a new handler and passes it to
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * outboundObserver.setOnReadyHandler(...)}.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, boolean, Iterator[])
	 *     constructor for param descriptions
	 */
	@SafeVarargs
	public static <MessageT> void copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		Iterator<MessageT>... messageProducers
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			messageProducers
		));
	}

	/**
	 * Calls {@link #copyWithFlowControl(CallStreamObserver, Executor, boolean, Iterator[])
	 * copyWithFlowControl(outboundObserver, taskExecutor, false, messageProducers)}.
	 */
	@SafeVarargs
	public static <MessageT> void copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		Iterator<MessageT>... messageProducers
	) {
		copyWithFlowControl(outboundObserver, taskExecutor, false, messageProducers);
	}

	/**
	 * Builds a message producing {@link Iterator} from {@code producerHasMoreMessagesIndicator} and
	 * {@code messageProducer} and calls
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
	 * copyWithFlowControl(outboundObserver, taskExecutor, builtIterator)}
	 */
	public static <MessageT> void copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		Supplier<Boolean> producerHasMoreMessagesIndicator,
		Supplier<MessageT> messageProducer
	) {
		copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			new Iterator<>() {
				@Override public boolean hasNext() { return producerHasMoreMessagesIndicator.get();}
				@Override public MessageT next() { return messageProducer.get(); }
				@Override public String toString() { return "single"; }
			}
		);
	}



	/**
	 * Calls {@link #DispatchingOnReadyHandler(CallStreamObserver, Executor, boolean, Iterator[])
	 * this(}{@code outboundObserver, taskExecutor, waitForOtherTasksToFinishOnException,
	 * }{@link #producerFunctionsToIterators(int, Function, Function, Function)
	 * producerFunctionsToIterators(numberOfTasks, producerHasMoreMessagesIndicator,
	 * messageProducer, messageProducerToString))}.
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		int numberOfTasks,
		Function<Integer, Boolean> producerHasMoreMessagesIndicator,
		Function<Integer, MessageT> messageProducer,
		Function<Integer, String> messageProducerToString
	) {
		this(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			producerFunctionsToIterators(
				numberOfTasks,
				producerHasMoreMessagesIndicator,
				messageProducer,
				messageProducerToString
			)
		);
	}

	/**
	 * Convenience function that constructs a new handler and passes it to
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * outboundObserver.setOnReadyHandler(...)}.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, boolean, int, Function,
	 *     Function, Function) constructor for param descriptions
	 */
	public static <MessageT> void copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		boolean waitForOtherTasksToFinishOnException,
		int numberOfTasks,
		Function<Integer, Boolean> producerHasMoreMessagesIndicator,
		Function<Integer, MessageT> messageProducer
	) {
		outboundObserver.setOnReadyHandler(new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			waitForOtherTasksToFinishOnException,
			numberOfTasks,
			producerHasMoreMessagesIndicator,
			messageProducer,
			Object::toString
		));
	}

	/**
	 * Calls
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, boolean, int, Function, Function)
	 * copyWithFlowControl(outboundObserver, taskExecutor, false, numberOfTasks,
	 * producerHasMoreMessagesIndicator, messageProducer}.
	 */
	public static <MessageT> void copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		Function<Integer, Boolean> producerHasMoreMessagesIndicator,
		Function<Integer, MessageT> messageProducer
	) {
		copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			false,
			numberOfTasks,
			producerHasMoreMessagesIndicator,
			messageProducer
		);
	}



	final CallStreamObserver<MessageT> outboundObserver;
	final Executor taskExecutor;
	final boolean waitForOtherTasksToFinishOnException;
	final Iterator<MessageT>[] messageProducers;

	final boolean[] taskRunning;
	final Object lock = new Object();
	int completedTaskCount = 0;
	Throwable error;



	/**
	 * For each task dispatches a handler of a single cycle of readiness of
	 * {@code outboundObserver}.
	 */
	public void run() {
		synchronized (lock) {
			if (completedTaskCount == messageProducers.length
					|| (error != null && !waitForOtherTasksToFinishOnException)) {
				return;
			}
			for (int taskNumber = 0; taskNumber < messageProducers.length; taskNumber++) {
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
		final Iterator<MessageT> taskProducer;



		TaskOnReadyHandler(int taskNumber) {
			this.taskNumber = taskNumber;
			taskProducer = messageProducers[taskNumber];
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
					if (++completedTaskCount < messageProducers.length) return;
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
							|| ++completedTaskCount == messageProducers.length) {
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



	/**
	 * Builds an array of length {@code numberOfTasks} of message producing {@link Iterator}s from
	 * {@code producerHasMoreMessagesIndicator}, {@code messageProducer} and
	 * {@code messageProducerToString} functions.
	 */
	public static <T> Iterator<T>[] producerFunctionsToIterators(
		int numberOfTasks,
		Function<Integer, Boolean> producerHasMoreMessagesIndicator,
		Function<Integer, T> messageProducer,
		Function<Integer, String> messageProducerToString
	) {
		@SuppressWarnings("unchecked")
		Iterator<T>[] producers = new Iterator[numberOfTasks];
		for (var i = 0; i < numberOfTasks; i++) {
			final var taskNumber = i;
			producers[taskNumber] = new Iterator<>() {

				@Override public boolean hasNext() {
					return !producerHasMoreMessagesIndicator.apply(taskNumber);
				}

				@Override public T next() {
					return messageProducer.apply(taskNumber);
				}

				@Override public String toString() {
					return messageProducerToString.apply(taskNumber);
				}
			};
		}
		return producers;
	}
}
