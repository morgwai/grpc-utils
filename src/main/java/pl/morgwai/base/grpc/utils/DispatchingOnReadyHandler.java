// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.*;

import io.grpc.stub.CallStreamObserver;



/**
 * An {@link CallStreamObserver#setOnReadyHandler(Runnable) onReadyHandler} that streams messages to
 * an outbound {@link CallStreamObserver} from multiple concurrent tasks with respect to
 * flow-control. Useful when processing of 1 inbound message may result in multiple outbound
 * messages that can be produced concurrently in multiple threads.<br/>
 * This class has a similar purpose to
 * {@link io.grpc.stub.StreamObservers#copyWithFlowControl(Iterator, CallStreamObserver)}, but work
 * is dispatched to an {@link Executor} supplied via {@code taskExecutor} constructor
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
	 * Convenience function that constructs a new handler and passes it to
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * outboundObserver.setOnReadyHandler(...)}.
	 * @return newly created handler.
	 * @see #DispatchingOnReadyHandler(CallStreamObserver, Executor, int, IntFunction, IntFunction,
	 *     String) constructor for param descriptions
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<? extends MessageT> messageProducer,
		IntFunction<Boolean> producerHasNextIndicator,
		String label
	) {
		final var handler = new DispatchingOnReadyHandler<MessageT>(
			outboundObserver,
			taskExecutor,
			numberOfTasks,
			messageProducer,
			producerHasNextIndicator,
			label
		);
		outboundObserver.setOnReadyHandler(handler);
		return handler;
	}

	/**
	 * Calls {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction,
	 * IntFunction, String) copyWithFlowControl(outboundObserver, taskExecutor, numberOfTasks,
	 * producerHasNextIndicator, messageProducer, messageProducer.toString())} (using
	 * {@code messageProducer.toString} as {@code label}).
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<? extends MessageT> messageProducer,
		IntFunction<Boolean> producerHasNextIndicator
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			numberOfTasks,
			messageProducer,
			producerHasNextIndicator,
			messageProducer.toString()
		);
	}

	/**
	 * Calls {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction,
	 * IntFunction, String) copyWithFlowControl(...)} with the number of tasks based on the length
	 * of {@code messageProducers} and message producing / indicating {@link IntFunction}s built
	 * from {@code messageProducers}' {@link Iterator#hasNext()} and {@link Iterator#next()}
	 * methods respectively. {@code messageProducers[0].toString()} is used as {@code label}.
	 */
	@SafeVarargs
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		Iterator<? extends MessageT>... messageProducers
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			messageProducers[0].toString(),
			messageProducers
		);
	}

	/**
	 * Calls {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction,
	 * IntFunction, String) copyWithFlowControl(...)} with the number of tasks based on the length
	 * of {@code messageProducers} and message producing / indicating {@link IntFunction}s built
	 * from {@code messageProducers}' {@link Iterator#hasNext()} and {@link Iterator#next()}
	 * methods respectively.
	 */
	@SafeVarargs
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		String label,
		Iterator<? extends MessageT>... messageProducers
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			messageProducers.length,
			(taskNumber) -> messageProducers[taskNumber].next(),
			(taskNumber) -> messageProducers[taskNumber].hasNext(),
			label
		);
	}

	/**
	 * Single task version of
	 * {@link #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction, IntFunction)}.
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		Supplier<? extends MessageT> messageProducer,
		Supplier<Boolean> producerHasNextIndicator
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			messageProducer,
			producerHasNextIndicator,
			messageProducer.toString()
		);
	}

	/**
	 * Single task version of {@link #copyWithFlowControl(CallStreamObserver, Executor, int,
	 * IntFunction, IntFunction, String)}.
	 */
	public static <MessageT> DispatchingOnReadyHandler<MessageT> copyWithFlowControl(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		Supplier<? extends MessageT> messageProducer,
		Supplier<Boolean> producerHasNextIndicator,
		String label
	) {
		return copyWithFlowControl(
			outboundObserver,
			taskExecutor,
			1,
			(always0) -> messageProducer.get(),
			(always0) -> producerHasNextIndicator.get(),
			label
		);
	}



	/**
	 * Constructs a new handler with {@code numberOfTasks} tasks and initializes message producing
	 * functions {@link #messageProducer} and {@link #producerHasNextIndicator} with
	 * respective params.
	 * Each task will be dispatched to {@code taskExecutor} and will produce messages by calling
	 * the {@link #messageProducer messageProducer.apply(taskNumber)}.<br/>
	 * Tasks are numbered from {@code 0} to {@code numberOfTasks - 1}.
	 * <p>
	 * Produced messages will be streamed concurrently from all the tasks to
	 * {@code outboundObserver} with respect to flow-control: if {@code outboundObserver} becomes
	 * unready, all the tasks will exit and will be redispatched after {@code outboundObserver}
	 * becomes ready again. Redispatching of a given task will continue until the task is marked as
	 * completed. After all the tasks are completed
	 * {@link CallStreamObserver#onCompleted() outboundObserver.onCompleted()} is called
	 * automatically.</p>
	 * <p>
	 * A task will be marked as completed if
	 * {@link #producerHasNextIndicator producerHasNextIndicator.apply(taskNumber)} returns
	 * {@code false} or if {@link #messageProducer messageProducer.apply(taskNumber)} throws a
	 * {@link NoSuchElementException}.</p>
	 * <p>
	 * If a task throws any other unchecked {@link Throwable}, it will be passed uncaught and the
	 * task will be left uncompleted. In such case it should be ensured that
	 * {@code outboundObserver} gets finalized in some way: for example {@link #messageProducer} may
	 * call {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(...)} before
	 * throwing.</p>
	 * <p>
	 * This constructor does <b>not</b> call {@code outboundObserver.setOnReadyHandler(this)}.</p>
	 * @param label for logging and debugging purposes.
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction, IntFunction)
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, Iterator[])
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, Supplier, Supplier)
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, int, IntFunction, IntFunction,
	 *     String)
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, String, Iterator[])
	 * @see #copyWithFlowControl(CallStreamObserver, Executor, Supplier, Supplier, String)
	 */
	public DispatchingOnReadyHandler(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		IntFunction<? extends MessageT> messageProducer,
		IntFunction<Boolean> producerHasNextIndicator,
		String label
	) {
		this.outboundObserver = outboundObserver;
		this.taskExecutor = taskExecutor;
		this.numberOfTasks = numberOfTasks;
		this.messageProducer = messageProducer;
		this.producerHasNextIndicator = producerHasNextIndicator;
		this.label = label;
		taskRunningOrCompleted = new boolean[numberOfTasks];
	}

	/**
	 * Constructor for those who prefer to override methods rather than provide functional handlers
	 * as params. Both {@link #producerHasNext(int)} and {@link #produceNextMessage(int)}
	 * must be overridden.
	 */
	protected DispatchingOnReadyHandler(
		CallStreamObserver<? super MessageT> outboundObserver,
		Executor taskExecutor,
		int numberOfTasks,
		String label
	) {
		this(outboundObserver, taskExecutor, numberOfTasks, null, null, label);
	}



	/**
	 * Produces a next message in the task with number {@code taskNumber}.
	 * The default implementation calls {@link #messageProducer}.
	 * @throws NoSuchElementException if the given task is already completed.
	 */
	protected MessageT produceNextMessage(int taskNumber) {
		return messageProducer.apply(taskNumber);
	}

	/**
	 * Called by the default implementation of {@link #produceNextMessage(int)}. Initialized
	 * via {@code messageProducer} {@link #DispatchingOnReadyHandler(
	 * CallStreamObserver, Executor, int, IntFunction, IntFunction, String) constructor} param.
	 */
	protected final IntFunction<? extends MessageT> messageProducer;



	/**
	 * Indicates whether {@link #produceNextMessage(int) produceNextMessage(taskNumber)} will
	 * produce more messages.
	 * The default implementation calls {@link #producerHasNextIndicator}.
	 * <p>
	 * Implementations are allowed to return {@code true} if it is hard to determine upfront if
	 * there will be more messages or not, and {@link #produceNextMessage(int)} may throw
	 * {@link NoSuchElementException} exception to indicate that the task is completed.<br/>
	 * Alternatively, this method may also block until it is able to give a definitive
	 * answer.</p>
	 */
	protected boolean producerHasNext(int taskNumber) {
		return producerHasNextIndicator.apply(taskNumber);
	}

	/**
	 * Called by the default implementation of {@link #producerHasNext(int)}. Initialized
	 * via {@code producerHasNextIndicator} {@link #DispatchingOnReadyHandler(
	 * CallStreamObserver, Executor, int, IntFunction, IntFunction, String) constructor} param.
	 */
	protected final IntFunction<Boolean> producerHasNextIndicator;



	/**
	 * Indicates that after all the message producing tasks are completed, {@code errorToReport}
	 * should be nevertheless reported via
	 * {@link CallStreamObserver#onError(Throwable) outboundObserver.onError(errorToReport)}.
	 * This method may be useful if one of the message producing tasks fails, but others
	 * should be allowed to continue regardless. If due to a such failure, the given task is unable
	 * to produce a message, then a call to this method within a {@link #messageProducer} should
	 * usually be followed by throwing a {@link NoSuchElementException} to mark the given task as
	 * completed.
	 */
	public final void reportErrorAfterTasksComplete(Throwable errorToReport) {
		synchronized (lock) {
			this.errorToReport = errorToReport;
		}
	}

	Throwable errorToReport;



	final CallStreamObserver<? super MessageT> outboundObserver;
	final Executor taskExecutor;
	final int numberOfTasks;
	final String label;

	final boolean[/*numberOfTasks*/] taskRunningOrCompleted;// prevents unnecessary task dispatching
	int completedTaskCount = 0;

	final Object lock = new Object();



	@Override
	public String toString() {
		return "DispatchingOnReadyHandler { label=\"" + label + "\" }";
	}



	/**
	 * Dispatches message producing tasks to {@code taskExecutor}. The tasks keep producing messages
	 * with {@link #produceNextMessage(int) produceNextMessage(taskNumber)} and sending them to
	 * {@code outboundObserver} as long as it is ready and the given task is not completed.
	 * When {@code outboundObserver} becomes unready, the tasks exit and are redispatched during
	 * the next call to this method when {@code outboundObserver} becomes ready again. Redispatching
	 * continues until a task is marked as completed.
	 */
	@Override
	public void run() {
		synchronized (lock) {
			if (completedTaskCount == numberOfTasks) return;
			for (int taskNumber = 0; taskNumber < numberOfTasks; taskNumber++) {
				// it may happen that outboundObserver will change its state from unready to ready
				// very fast, before some tasks can even notice. Such tasks will span over more than
				// 1 cycle and taskRunning[] flags prevent dispatching duplicates in such case.
				if (taskRunningOrCompleted[taskNumber]) continue;
				taskRunningOrCompleted[taskNumber] = true;
				taskExecutor.execute(new Task(taskNumber));
			}
		}
	}



	/**
	 * Handles a single cycle of {@link #outboundObserver}'s readiness for the
	 * {@link #messageProducer} with the number given by the {@link #Task(int) constructor} param.
	 * Each task will be redispatched to {@link #taskExecutor} each time {@link #outboundObserver}
	 * becomes ready until {@link #producerHasNextIndicator} applied to task's number
	 * returns {@code false} or until {@link #messageProducer} applied to task's number throws a
	 * {@link NoSuchElementException}.
	 */
	class Task implements Runnable {

		final int taskNumber;
		Task(int taskNumber) { this.taskNumber = taskNumber; }



		@Override public void run() {
			try {
				// only outboundObserver async methods and handler's bookkeeping state manipulation
				// happen inside lock, while possibly slow producer methods happen outside of lock
				while (producerHasNext(taskNumber)) {
					synchronized (lock) {
						if ( !outboundObserver.isReady()) {
							// "pause" the loop until the task is redispatched when outboundObserver
							// becomes ready again
							taskRunningOrCompleted[taskNumber] = false;
							return;
						}
					}
					final var message = produceNextMessage(taskNumber);  // outside of lock
					synchronized (lock) {
						outboundObserver.onNext(message);
					}
				}
			} catch (NoSuchElementException e) {/* equivalent to !producerHasNext(taskNumber) */}
			// taskRunningOrCompleted[taskNumber] is always true at this point

			// task completed, perform final bookkeeping
			synchronized (lock) {
				completedTaskCount++;
				if (completedTaskCount < numberOfTasks) return;
				if (errorToReport == null) {
					outboundObserver.onCompleted();
				} else {
					outboundObserver.onError(errorToReport);
				}
			}
		}



		@Override public String toString() {
			return "DispatchingOnReadyHandler.Task { label=\"" + label + "\", taskNumber="
					+ taskNumber + " }";
		}
	}
}
