// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.*;

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
	 * <p>
	 * If any task throws {@link NoSuchElementException}, it will treated the same way as if a call
	 * to its {@link Iterator#hasNext() hasNext()} method returned {@code false} and the task will
	 * be marked as completed.<br/>
	 * Any other unchecked {@link Throwable} will be passed uncaught and task will be left
	 * uncompleted. In such case it should be ensured that the call gets aborted in some way, for
	 * example the given {@code messageProducer} may call
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(...)} before throwing
	 * an unchecked {@link Throwable}s other than {@link NoSuchElementException}.</p>
	 * @param outboundObserver target outbound observer to which messages from
	 *     {@code messageProducers} will be streamed.
	 * @param taskExecutor executor to which message producing tasks will be dispatched.
	 * @param messageProducers each element produces in a separate task (dispatched to
	 *     {@code taskExecutor}), messages that will be streamed to {@code outboundObserver}.
	 */
	@SafeVarargs
	public DispatchingOnReadyHandler(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		Iterator<MessageT>... messageProducers
	) {
		this.messageProducers = messageProducers;
		this.outboundObserver = outboundObserver;
		this.taskExecutor = taskExecutor;
		taskRunning = new boolean[messageProducers.length];
	}

	/**
	 * Convenience function that constructs a new handler and passes it to
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * outboundObserver.setOnReadyHandler(...)}.
	 * @return newly created handler.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, Iterator[])
	 *     constructor for param descriptions
	 */
	@SafeVarargs
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		Iterator<MessageT>... messageProducers
	) {
		final var handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			messageProducers
		);
		outboundObserver.setOnReadyHandler(handler);
		return handler;
	}

	/**
	 * Builds a message producing {@link Iterator} from {@code producerHasMoreMessagesIndicator} and
	 * {@code messageProducer} and calls
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
	 * copyWithFlowControl(outboundObserver, taskExecutor, builtIterator)}
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		Supplier<Boolean> producerHasMoreMessagesIndicator,
		Supplier<MessageT> messageProducer
	) {
		return copyWithFlowControl(
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
	 * Builds an array of message producing {@link Iterator}s using
	 * {@link #producerFunctionsToIterators(int, IntFunction, IntFunction, IntFunction)} and calls
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
	 * copyWithFlowControl(outboundObserver, taskExecutor, builtIterators)}
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<Boolean> producerHasMoreMessagesIndicator,
		IntFunction<MessageT> messageProducer
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			producerFunctionsToIterators(
				numberOfTasks,
				producerHasMoreMessagesIndicator,
				messageProducer,
				String::valueOf
			)
		);
	}



	final CallStreamObserver<MessageT> outboundObserver;
	final Executor taskExecutor;
	final Iterator<MessageT>[] messageProducers;

	final boolean[] taskRunning;
	int completedTaskCount = 0;
	Throwable errorToReport;

	final Object lock = new Object();



	/**
	 * Indicates that after all tasks are completed {@code errorToReport} should be nevertheless
	 * reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(errorToReport)}. A call
	 * to this method within a {@code messageProcessor} is usually followed by throwing a
	 * {@link NoSuchElementException}.
	 */
	public final void reportErrorAfterTasksComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
	}



	/**
	 * For each task dispatches a handler of a single cycle of readiness of
	 * {@code outboundObserver}.
	 */
	public void run() {
		synchronized (lock) {
			if (completedTaskCount == messageProducers.length) {
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
			boolean ready;
			synchronized (lock) {
				ready = outboundObserver.isReady();
			}
			while (ready && taskProducer.hasNext()) {
				try {
					final var result = taskProducer.next();
					synchronized (lock) {
						outboundObserver.onNext(result);
						ready = outboundObserver.isReady();
					}
				} catch (NoSuchElementException e) {
					break;  // treat NoSuchElementException same as !hasNext()
				}
			}
			if ( !ready) {
				taskRunning[taskNumber] = false;
				return;
			}
			// taskRunning[taskNumber] will be left true to not respawn completed/aborted tasks
			synchronized (lock) {
				if (++completedTaskCount < messageProducers.length) return;
				if (errorToReport == null) {
					outboundObserver.onCompleted();
				} else {
					outboundObserver.onError(errorToReport);
				}
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
		IntFunction<Boolean> producerHasMoreMessagesIndicator,
		IntFunction<T> messageProducer,
		IntFunction<String> messageProducerToString
	) {
		@SuppressWarnings("unchecked")
		Iterator<T>[] producers = new Iterator[numberOfTasks];
		for (var i = 0; i < numberOfTasks; i++) {
			final var taskNumber = i;
			producers[taskNumber] = new Iterator<>() {

				@Override public boolean hasNext() {
					return producerHasMoreMessagesIndicator.apply(taskNumber);
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
