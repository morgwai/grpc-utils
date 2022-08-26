// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.*;

import io.grpc.stub.CallStreamObserver;



/**
 * Streams messages to an {@link CallStreamObserver outboundObserver} from multiple sources in
 * separate threads with respect to flow-control. Useful in sever methods when 1 request message can
 * result in multiple response messages that can be produced concurrently in separate tasks.
 * This class has similar purpose to
 * {@link io.grpc.stub.StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)}, but work
 * is dispatched to the {@link #taskExecutor Executor} supplied via {@code taskExecutor} constructor
 * param and parallelized according to the value of {@code numberOfTasks} constructor param.
 * <p>
 * Typical usage in streaming-server methods:</p>
 * <pre>
 * public void myServerStreamingMethod(
 *         RequestMessage request, StreamObserver&lt;ResponseMessage&gt; basicResponseObserver) {
 *     final var processor = new MyRequestProcessor(request, NUMBER_OF_TASKS);
 *     final var responseObserver =
 *             (ServerCallStreamObserver&lt;ResponseMessage&gt;) basicResponseObserver;
 *     responseObserver.setOnCancelHandler(processor::cancel);
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
	 * Indicates whether the {@link #produceNextMessage(int) produceNextMessage(taskNumber)} will
	 * produce more messages for the task number {@code taskNumber}.
	 * The default implementation calls {@link #producerHasMoreMessagesIndicator}.
	 * <p>
	 * Implementations are allowed to return {@code true} if it is hard to determine upfront if
	 * there will be more messages or not, and {@link #produceNextMessage(int)} may throw
	 * {@link NoSuchElementException} exception to indicate that the task is completed.<br/>
	 * Alternatively, this method is also free to block until it is able to give a definitive
	 * answer.</p>
	 */
	protected boolean producerHasMoreMessages(int taskNumber) {
		return producerHasMoreMessagesIndicator.apply(taskNumber);
	}

	/**
	 * Called by the default implementation of {@link #producerHasMoreMessages(int)}. Initialized
	 * via {@code producerHasMoreMessagesIndicator} {@link #DispatchingOnReadyHandler(
	 * CallStreamObserver, Executor, int, IntFunction, IntFunction) constructor} param.
	 */
	protected final IntFunction<Boolean> producerHasMoreMessagesIndicator;



	/**
	 * Produces a next message in the task number {@code taskNumber}.
	 * The default implementation calls {@link #messageProducer}.
	 * @throws NoSuchElementException if the task number {@code taskNumber} is completed.
	 */
	protected MessageT produceNextMessage(int taskNumber) {
		return messageProducer.apply(taskNumber);
	}

	/**
	 * Called by the default implementation of {@link #produceNextMessage(int)}. Initialized
	 * via {@code messageProducer} {@link #DispatchingOnReadyHandler(
	 * CallStreamObserver, Executor, int, IntFunction, IntFunction) constructor} param.
	 */
	protected final IntFunction<MessageT> messageProducer;



	/**
	 * Constructs a new handler with {@code numberOfTasks} tasks and initializes result message
	 * producing functions {@link #messageProducer} and {@link #producerHasMoreMessagesIndicator}.
	 * Each task will be dispatched to {@code taskExecutor} and will produce messages by applying
	 * the {@link #messageProducer IngFunction} supplied via {@code messageProducer} to the task's
	 * number.<br/>
	 * Tasks are numbered from {@code 0} to {@code numberOfTasks - 1}.
	 * <p>
	 * Resulting messages will be streamed concurrently from all tasks to {@code outboundObserver}
	 * param with respect to flow-control: if {@code outboundObserver} becomes unready, the tasks
	 * will exit and will be redispatched again after {@code outboundObserver} becomes ready again.
	 * Redispatching will continue until the given task is completed.</p>
	 * <p>
	 * A task will be marked as completed if applying the {@link #producerHasMoreMessagesIndicator
	 * IntFunction} supplied via {@code producerHasMoreMessagesIndicator} param to task's number
	 * returns {@code false} or if {@link #messageProducer} throws {@link NoSuchElementException}
	 * for the given task number.</p>
	 * <p>
	 * If a task throws any other unchecked {@link Throwable}, it will be passed uncaught and the
	 * task will be left uncompleted. In such case it should be ensured that the call gets aborted
	 * in some way: for example {@link #messageProducer} may call
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(...)} before
	 * throwing.</p>
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<Boolean> producerHasMoreMessagesIndicator,
		IntFunction<MessageT> messageProducer
	) {
		this.outboundObserver = outboundObserver;
		this.taskExecutor = taskExecutor;
		this.numberOfTasks = numberOfTasks;
		this.producerHasMoreMessagesIndicator = producerHasMoreMessagesIndicator;
		this.messageProducer = messageProducer;
		taskRunning = new boolean[numberOfTasks];
	}

	/**
	 * Constructor for those who prefer to override methods rather than provide lambdas as params.
	 * Both {@link #producerHasMoreMessages(int)} and {@link #produceNextMessage(int)} must be
	 * overridden.
	 */
	protected DispatchingOnReadyHandler(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks
	) {
		this(outboundObserver, taskExecutor, numberOfTasks, null, null);
	}



	/**
	 * Convenience function that constructs a new handler and passes it to
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * outboundObserver.setOnReadyHandler(...)}.
	 * @return newly created handler.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, IntFunction, IntFunction)
	 *     constructor for param descriptions
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<Boolean> producerHasMoreMessagesIndicator,
		IntFunction<MessageT> messageProducer
	) {
		final var handler = new DispatchingOnReadyHandler<>(
			outboundObserver,
			taskExecutor,
			numberOfTasks,
			producerHasMoreMessagesIndicator,
			messageProducer
		);
		outboundObserver.setOnReadyHandler(handler);
		return handler;
	}

	/**
	 * Calls {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction,
	 * IntFunction)} with a number of tasks based on the length of {@code messageProducers} and
	 * {@link IntFunction}s built from {@code messageProducers}' {@link Iterator#hasNext()} and
	 * {@link Iterator#next()} methods.
	 */
	@SafeVarargs
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		Iterator<MessageT>... messageProducers
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			messageProducers.length,
			(taskNumber) -> messageProducers[taskNumber].hasNext(),
			(taskNumber) -> messageProducers[taskNumber].next()
		);
	}

	/**
	 * Single task version of
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction, IntFunction)}.
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		Supplier<Boolean> producerHasMoreMessagesIndicator,
		Supplier<MessageT> messageProducer
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			1,
			(always0) -> producerHasMoreMessagesIndicator.get(),
			(always0) -> messageProducer.get()
		);
	}



	final CallStreamObserver<? super MessageT> outboundObserver;
	final Executor taskExecutor;
	final int numberOfTasks;

	final boolean[] taskRunning;
	int completedTaskCount = 0;
	Throwable errorToReport;

	final Object lock = new Object();



	/**
	 * Indicates that after all tasks are completed {@code errorToReport} should be nevertheless
	 * reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(errorToReport)}. A call
	 * to this method within a {@code messageProcessor} is usually followed by throwing a
	 * {@link NoSuchElementException} to mark it as completed.
	 */
	public final void reportErrorAfterTasksComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
	}



	/**
	 * Dispatches {@code numberOfTasks} tasks to {@code taskExecutor}, that keep producing
	 * messages with {@link #produceNextMessage(int) produceNextMessage(taskNumber)} and sending
	 * them to {@code outboundObserver} as long as it is ready and the given task is not completed.
	 * When {@code outboundObserver} becomes unready, tasks exit and will be redispatched during
	 * the next call to this method. Redispatching will continue until tasks are marked as
	 * completed.
	 */
	public void run() {
		synchronized (lock) {
			if (completedTaskCount == numberOfTasks) {
				return;
			}
			for (int taskNumber = 0; taskNumber < numberOfTasks; taskNumber++) {
				// it may happen that responseObserver will change its state from unready to ready
				// very fast, before some tasks can even notice. Such tasks will span over more than
				// 1 cycle and taskRunning flags prevent dispatching duplicates in in such case.
				if (taskRunning[taskNumber]) continue;
				taskRunning[taskNumber] = true;
				taskExecutor.execute(new Task(taskNumber));
			}
		}
	}



	/**
	 * Handles a single cycle of {@link #outboundObserver}'s readiness for the
	 * {@link #messageProducer} with the number given by the {@link #Task(int) constructor} param.
	 * Each task will be redispatched to {@link #taskExecutor} each time {@link #outboundObserver}
	 * becomes ready until {@link #producerHasMoreMessagesIndicator} returns {@code false} for the
	 * task's number or until {@link #messageProducer} throws {@link NoSuchElementException} for
	 * the task's number.
	 */
	class Task implements Runnable {

		final int taskNumber;
		Task(int taskNumber) { this.taskNumber = taskNumber; }



		@Override public void run() {
			synchronized (lock) {
				taskRunning[taskNumber] = outboundObserver.isReady();
				if ( !taskRunning[taskNumber]) return;
			}
			while (producerHasMoreMessages(taskNumber)) {
				try {
					final var result = produceNextMessage(taskNumber);
					synchronized (lock) {
						outboundObserver.onNext(result);
						taskRunning[taskNumber] = outboundObserver.isReady();
						if ( !taskRunning[taskNumber]) return;
					}
				} catch (NoSuchElementException e) {
					break;  // treat NoSuchElementException same as !hasNext()
				}
			}
			// taskRunning[taskNumber] will be left true to not respawn completed/aborted tasks
			synchronized (lock) {
				if (++completedTaskCount < numberOfTasks) return;
				if (errorToReport == null) {
					outboundObserver.onCompleted();
				} else {
					outboundObserver.onError(errorToReport);
				}
			}
		}



		@Override public String toString() {
			return "onReadyHandlerTask-" + taskNumber;
		}
	}
}
