// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;



/**
 * A fake {@link ServerCallStreamObserver} testing helper class.
 * Helps to emulate behavior of a client and the gRPC system in server-side infrastructure code
 * tests.<br/>
 * <br/>
 * <b>Note:</b> in most cases it is better to use {@link io.grpc.inprocess.InProcessChannelBuilder}
 * for testing gRPC methods. This class is mainly intended for testing infrastructure parts.<br/>
 * <br/>
 * Usage:<ol>
 *   <li>Configure observer's readiness by adjusting {@link #outputBufferSize} and
 *     {@link #unreadyDurationMillis} variables.</li>
 *   <li>Depending on client type (unary/streaming) of your gRPC method pass it to one of
 *     {@link #callWithinListenerLock(Consumer)},
 *     {@link #callWithinListenerLock(Function, Consumer)} methods.</li>
 *   <li>{@link #awaitFinalization(long)} can be used to wait until {@link #onCompleted()} or
 *     {@link #onError(Throwable)} is called.</li>
 *   <li>Client canceling can be simulated using {@link #cancel()} method.</li>
 *   <li>Results can be verified with {@link #getOutputData()}, {@link #getFinalizedCount()},
 *     {@link #getReportedError()} methods and by shutting down and inspecting
 *     {@link FailureTrackingThreadPoolExecutor} supplied to the constructor.</li>
 * </ol>
 */
public class FakeResponseObserver<ResponseT>
		extends ServerCallStreamObserver<ResponseT> {



	/**
	 * @param grpcInternalExecutor executor for gRPC internal tasks, such as marking response
	 * observer as ready, delivering requested messages etc. Its pool size should be not smaller
	 * than the number of requests concurrently processed by the code under test (usually determined
	 * by the argument of the initial call to {@link ServerCallStreamObserver#request(int)}.
	 */
	public FakeResponseObserver(FailureTrackingThreadPoolExecutor grpcInternalExecutor) {
		this.grpcInternalExecutor = grpcInternalExecutor;
	}

	final FailureTrackingThreadPoolExecutor grpcInternalExecutor;



	/**
	 * List of arguments of calls to {@link #onNext(Object)}.
	 */
	public List<ResponseT> getOutputData() { return outputData; }
	final List<ResponseT> outputData = new LinkedList<>();

	/**
	 * Response observer becomes unready after each <code>outputBufferSize</code> messages are
	 * submitted to it. Default is <code>0</code> which means always ready.
	 */
	public volatile int outputBufferSize = 0;

	/**
	 * Duration for which observer will be unready. By default 1ms.
	 */
	public volatile long unreadyDurationMillis = 1l;



	final Object listenerLock = new Object();



	/**
	 * Calls {@code unaryClientMethod} within listener's lock.
	 */
	public void callWithinListenerLock(Consumer<StreamObserver<ResponseT>> unaryClientMethod) {
		synchronized (listenerLock) {
			unaryClientMethod.accept(this);
			if (onReadyHandler != null) onReadyHandler.run();
		}
	}



	/**
	 * Calls {@code streamingClientMethod} within listener's lock and delivers request messages
	 * to returned request observer from {@code requestProducer}.
	 * @param requestProducer dispatched to {@link #grpcInternalExecutor} whenever
	 *        {@link #request(int)} method is called. It should usually call its argument's
	 *        {@link StreamObserver#onNext(Object)} possibly followed by
	 *        {@link StreamObserver#onCompleted()} or {@link StreamObserver#onError(Throwable)} to
	 *        simulate client's behavior. It may sleep arbitrarily long to simulate before the above
	 *        calls to simulate client's or network delay.
	 */
	public <RequestT> void callWithinListenerLock(
			Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> streamingClientMethod,
			Consumer<StreamObserver<RequestT>> requestProducer) {
		StreamObserver<RequestT> requestObserver;
		synchronized (listenerLock) {
			requestObserver = streamingClientMethod.apply(this);
		}
		startRequestDelivery(requestObserver, requestProducer);
		if (autoRequest) requestOne();
	}



	/**
	 * For low level testing of onReady and onCancel handlers.
	 */
	public void runWithinListenerLock(Runnable handler) {
		synchronized (listenerLock) {
			handler.run();
		}
	}



	/**
	 * Verifies that at most 1 thread calls this observer's methods concurrently.
	 */
	final LoggingReentrantLock concurrencyGuard = new LoggingReentrantLock();



	@Override
	public void onNext(ResponseT message) {
		if ( ! concurrencyGuard.tryLock("onNext")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			// Currently, the behavior of "the real" responseObserver is inconsistent and depends
			// on which thread onNext() is called on.
			// See https://github.com/grpc/grpc-java/issues/8409
			if (cancelled && onCancelHandler == null) throw Status.CANCELLED.asRuntimeException();

			if (log.isLoggable(Level.FINER)) log.finer("response sent: " + message);
			outputData.add(message);

			// mark observer unready and schedule becoming ready again
			if (outputBufferSize > 0 && (outputData.size() % outputBufferSize == 0)) {
				log.fine("response observer unready");
				ready = false;
				grpcInternalExecutor.execute(new Runnable() {

					@Override public void run() { markObserverReady(unreadyDurationMillis); }

					@Override public String toString() { return "readyMarker"; }
				});
			}
		} finally {
			concurrencyGuard.unlock();
		}
	}

	private void markObserverReady(long delayMillis) {
		try {
			Thread.sleep(delayMillis);
		} catch (InterruptedException e) {}
		synchronized (listenerLock) {
			if ( ! ready) {
				log.fine("response observer ready");
				ready = true;
				if (onReadyHandler != null) onReadyHandler.run();
			}
		}
	}



	@Override
	public boolean isReady() {
		if ( ! concurrencyGuard.tryLock("isReady")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			return ready;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	volatile boolean ready = true;



	@Override
	public void setOnReadyHandler(Runnable onReadyHandler) {
		if ( ! concurrencyGuard.tryLock("setOnReadyHandler")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.onReadyHandler = onReadyHandler;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	Runnable onReadyHandler;



	/**
	 * Count of calls to {@link #onCompleted()} and {@link #onError(Throwable)}.
	 * Should be 1 at the end of positive test methods.
	 */
	public int getFinalizedCount() { return finalizedCount; }
	int finalizedCount = 0;

	/**
	 * Should an AssertionError be thrown immediately upon second finalization.
	 * By default <code>false</code>.
	 */
	public boolean failOnMultipleFinalizations = false;

	final Object finalizationGuard = new Object();



	@Override
	public void onCompleted() {
		if ( ! concurrencyGuard.tryLock("onCompleted")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			log.fine("response completed");
			synchronized (finalizationGuard) {
				finalizedCount++;
				if (finalizedCount > 1 && failOnMultipleFinalizations) {
					throw new AssertionError("multipe finalizations");
				}
				finalizationGuard.notify();
			}
		} finally {
			concurrencyGuard.unlock();
		}
	}



	@Override
	public void onError(Throwable t) {
		if ( ! concurrencyGuard.tryLock("onError")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			if (log.isLoggable(Level.FINE)) log.fine("error reported: " + t);
			synchronized (finalizationGuard) {
				reportedError = t;
				finalizedCount++;
				if (finalizedCount > 1 && failOnMultipleFinalizations) {
					throw new AssertionError("multipe finalizations");
				}
				finalizationGuard.notify();
			}
		} finally {
			concurrencyGuard.unlock();
		}
	}

	/**
	 * Stored argument of {@link #onError(Throwable)}.
	 */
	public Throwable getReportedError() { return reportedError; }
	Throwable reportedError;



	/**
	 * Awaits until finalization (call to either {@link #onCompleted()} or
	 * {@link #onError(Throwable)}) occurs or timeout exceeds.
	 */
	public void awaitFinalization(long timeoutMillis) throws InterruptedException {
		synchronized (finalizationGuard) {
			if (finalizedCount == 0 && reportedError == null) finalizationGuard.wait(timeoutMillis);
		}
		if (finalizedCount == 0 && reportedError == null) throw new RuntimeException("timeout");
	}



	@Override
	public void disableAutoRequest() {
		if ( ! concurrencyGuard.tryLock("disableAutoRequest")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.autoRequest = false;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	@Override
	public void disableAutoInboundFlowControl() {
		disableAutoRequest();
	}

	boolean autoRequest = true;



	/**
	 * Sets up delivery of request messages from {@code requestProducer} to {@code requestObserver}
	 * (test subject) whenever {@link #request(int)} method is called.<br/>
	 * Also delivers messages for all accumulated {@link #request(int)} calls that happened before
	 * this method was called.
	 * <p>
	 * Note: this method is exposed for low level testing of stand-alone request observers. It is
	 * automatically called by {@link #callWithinListenerLock(Function, Consumer)} which should be
	 * used for normal gRPC method testing.</p>
	 */
	@SuppressWarnings("unchecked")
	<RequestT> void startRequestDelivery(
			StreamObserver<RequestT> requestObserver,
			Consumer<StreamObserver<RequestT>> requestProducer) {
		this.requestProducer = (Consumer<StreamObserver<?>>)(Consumer<?>) requestProducer;
		this.requestObserver = new StreamObserver<RequestT>() {

			@Override
			public void onNext(RequestT message) {
				synchronized(listenerLock) {
					requestObserver.onNext(message);
				}
			}

			@Override
			public void onError(Throwable error) {
				synchronized(listenerLock) {
					requestObserver.onError(error);
				}
			}

			@Override
			public void onCompleted() {
				synchronized(listenerLock) {
					requestObserver.onCompleted();
				}
			}
		};
		request(accumulatedMessageRequestCount);
	}

	volatile Consumer<StreamObserver<?>> requestProducer;
	StreamObserver<?> requestObserver;
	int accumulatedMessageRequestCount = 0;



	@Override
	public void request(int count) {
		if (autoRequest) throw new AssertionError("autoRequest was not disabled");
		if (requestProducer == null) {
			accumulatedMessageRequestCount += count;
			return;
		}
		for (int i = 0; i < count; i++) requestOne();
	}

	void requestOne() {
		grpcInternalExecutor.execute(new Runnable() {

			@Override public void run() {
				requestProducer.accept(requestObserver);
				if (autoRequest) {
					synchronized (finalizationGuard) {
						if (finalizedCount == 0) requestOne();
					}
				}
			}

			@Override public String toString() {
				return "requestProducer " + requestProducer.toString();
			}
		});
	}


	/**
	 * Simulates client canceling a call by a client.
	 */
	public void cancel() {
		cancelled = true;
		synchronized (listenerLock) {
			if (onCancelHandler != null) onCancelHandler.run();
		}
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	volatile boolean cancelled = false;



	@Override
	public void setOnCancelHandler(Runnable onCancelHandler) {
		if ( ! concurrencyGuard.tryLock("setOnCancelHandler")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.onCancelHandler = onCancelHandler;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	Runnable onCancelHandler;



	@Override
	public void setCompression(String compression) {
		if ( ! concurrencyGuard.tryLock("setCompression")) {
			throw new AssertionError("concurrency violation");
		}
		concurrencyGuard.unlock();
	}

	@Override
	public void setMessageCompression(boolean enable) {
		if ( ! concurrencyGuard.tryLock("setMessageCompression")) {
			throw new AssertionError("concurrency violation");
		}
		concurrencyGuard.unlock();
	}



	/**
	 * Tracks task scheduling failures, occurring mainly on attempts to execute a task after
	 * the executor was shutdown.
	 */
	public static class FailureTrackingThreadPoolExecutor extends ThreadPoolExecutor {



		@Override
		public void execute(Runnable task) {
			try {
				super.execute(task);
			} catch (Exception e) {
				log.log(Level.SEVERE, "failure submitting " + task.toString(), e);
				submissionFailures.add(new SubmissionFailure(task, e));
			}
		}

		/**
		 * List of all submission failures that occurred.
		 */
		public List<SubmissionFailure> getSubmissionFailures() { return submissionFailures; }
		List<SubmissionFailure> submissionFailures = new LinkedList<>();



		public FailureTrackingThreadPoolExecutor(int poolSize) {
			super(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
		}



		public static class SubmissionFailure {

			public Runnable task;
			public Exception exception;

			public SubmissionFailure(Runnable task, Exception exception) {
				this.task = task;
				this.exception = exception;
			}
		}
	}



	@SuppressWarnings("serial")
	static class LoggingReentrantLock extends ReentrantLock {

		List<String> labels = new LinkedList<>();

		public boolean tryLock(String label) {
			boolean result = tryLock();
			if (result) {
				labels.add(label);
				if (log.isLoggable(Level.FINEST) ) {
					StringBuilder lockLog = new StringBuilder("locked   ");
					for (final var lockLabel: labels) lockLog.append(lockLabel).append('.');
					log.log(Level.FINEST, lockLog.toString(), new Exception());
				}
			} else {
				log.log(Level.SEVERE, "failed to lock " + label, new Exception());
			}
			return result;
		}

		@Override
		public void unlock() {
			if (log.isLoggable(Level.FINEST) ) {
				StringBuilder lockLog = new StringBuilder("unlocked ");
				for (final var lockLabel: labels) lockLog.append(lockLabel).append('.');
				log.log(Level.FINEST, lockLog.toString(), new Exception());
			}
			labels.remove(labels.size() - 1);
			super.unlock();
		}
	}



	/**
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message sent to the observer.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static final Logger log = Logger.getLogger(FakeResponseObserver.class.getName());
	public static Logger getLogger() { return log; }
}