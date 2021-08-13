// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
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
 *   <li>Configure observer's readiness can be controlled by adjusting {@link #outputBufferSize} and
 *     {@link #unreadyDurationMillis} variables.</li>
 *   <li>Pass the observer to your gRPC method under test.</li>
 *   <li>For BiDi, a message producer and a request observer obtained from the method under test
 *     must be supplied using {@link #setBiDi(StreamObserver, Consumer)} method.<br/>
 *     Client canceling a call can be simulated using {@link #cancel()} method.</li>
 *   <li>If the method under test dispatches work to other threads, {@link #awaitFinalization(long)}
 *     can be used to wait until {@link #onCompleted()} or {@link #onError(Throwable)} is called.
 *     </li>
 *   <li>Results can be verified with {@link #getOutputData()}, {@link #getFinalizedCount()},
 *     {@link #getReportedError()} methods and by shutting down and inspecting
 *     {@link FailureTrackingThreadPoolExecutor} supplied to the constructor.</li>
 * </ol>
 */
public class FakeResponseObserver<ResponseT>
		extends ServerCallStreamObserver<ResponseT> {



	/**
	 * @param grpcInternalExecutor executor for gRPC internal tasks, such as marking response
	 * observer as ready, delivering requested messages etc.
	 */
	public FakeResponseObserver(FailureTrackingThreadPoolExecutor grpcInternalExecutor) {
		this.grpcInternalExecutor = grpcInternalExecutor;
	}

	FailureTrackingThreadPoolExecutor grpcInternalExecutor;



	/**
	 * List of arguments of calls to {@link #onNext(Object)}.
	 */
	public List<ResponseT> getOutputData() { return outputData; }
	List<ResponseT> outputData = new LinkedList<>();

	/**
	 * Response observer becomes unready after each <code>outputBufferSize</code> messages are
	 * submitted to it. Default is <code>0</code> which means always ready.
	 */
	public volatile int outputBufferSize = 0;

	/**
	 * Duration for which observer will be unready. By default 1ms.
	 */
	public volatile long unreadyDurationMillis = 1l;



	/**
	 * Verifies that at most 1 thread calls this observer's methods concurrently.
	 */
	LoggingReentrantLock concurrencyGuard = new LoggingReentrantLock();

	/**
	 * Ensures that user's request observer will be called by at most 1 thread concurrently.
	 */
	Object listenerLock = new Object();



	@Override
	public void onNext(ResponseT message) {
		if ( ! concurrencyGuard.tryLock("onNext")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			if (log.isLoggable(Level.FINER)) log.finer("response sent: " + message);
			if (cancelled) throw Status.CANCELLED.asRuntimeException();
			// TODO: some other methods probably should check cancel state also:
			// verify which ones and fix it.

			outputData.add(message);

			// mark observer unready and schedule becoming ready again
			if (outputBufferSize > 0 && (outputData.size() % outputBufferSize == 0)) {
				log.fine("response observer unready");
				ready = false;
				grpcInternalExecutor.execute(() -> markObserverReady(unreadyDurationMillis));
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
				onReadyHandler.run();
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
	 * Awaits until finalization (call to either {@link #onCompleted()} or
	 * {@link #onError(Throwable)}) occurs or timeout exceeds.
	 */
	public void awaitFinalization(long timeoutMillis) throws InterruptedException {
		synchronized (finalizationGuard) {
			if (finalizedCount == 0 && reportedError == null) finalizationGuard.wait(timeoutMillis);
		}
	}

	Object finalizationGuard = new Object();



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



	@Override
	public void disableAutoRequest() {
		if ( ! concurrencyGuard.tryLock("disableAutoRequest")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.autoRequestDisabled = true;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	@Override
	public void disableAutoInboundFlowControl() {
		disableAutoRequest();
	}

	boolean autoRequestDisabled = false;



	/**
	 * Configures this {@code FakeResponseObserver} to deliver request messages to the supplied
	 * {@code requestObserver} (test subject) whenever {@link #request(int)} method is called. Sets
	 * necessary synchronization between calls from {@code requestProducer} to
	 * {@code requestObserver} and other parts of gRPC system to ensure obeying all concurrency
	 * contracts.
	 * @param requestObserver observer under test to which request messages should be delivered.
	 * @param requestProducer dispatched to {@link #grpcInternalExecutor} whenever
	 *        {@link #request(int)} method is called. It should usually call its argument's
	 *        {@link StreamObserver#onNext(Object)} optionally followed by
	 *        {@link StreamObserver#onCompleted()} or {@link StreamObserver#onError(Throwable)} to
	 *        simulate a client behavior.
	 */
	@SuppressWarnings("unchecked")
	<RequestT> void setBiDi(
			StreamObserver<RequestT> requestObserver,
			Consumer<StreamObserver<RequestT>> requestProducer) {
		this.requestProducer = (Consumer<StreamObserver<?>>)(Object) requestProducer;
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

	Consumer<StreamObserver<?>> requestProducer;
	StreamObserver<?> requestObserver;



	@Override
	public void request(int count) {
		if ( ! autoRequestDisabled) throw new AssertionError("autoRequest was not disabled");
		if (requestProducer != null) {
			for (int i = 0; i < count; i++) {
				grpcInternalExecutor.execute(() -> requestProducer.accept(requestObserver));
			}
		} else {
			accumulatedMessageRequestCount += count;
		}
	}

	int accumulatedMessageRequestCount = 0;



	/**
	 * Simulates client canceling a call by a client.
	 */
	public void cancel() {
		synchronized (listenerLock) {
			cancelled = true;
			if (onCancelHandler != null) onCancelHandler.run();
		}
	}

	@Override
	public boolean isCancelled() {
		if ( ! concurrencyGuard.tryLock("isCancelled")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			return cancelled;
		} finally {
			concurrencyGuard.unlock();
		}
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
		public void execute(Runnable command) {
			try {
				super.execute(command);
			} catch (Exception e) {
				failure = true;
			}
		}



		/**
		 * {@code true} if there were attempts to execute more tasks after shutdown.
		 */
		public boolean hadFailures() { return failure; }
		volatile boolean failure = false;



		public FailureTrackingThreadPoolExecutor(int poolSize) {
			super(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
		}
	}



	@SuppressWarnings("serial")
	static class LoggingReentrantLock extends ReentrantLock {

		List<String> labels = new LinkedList<>();

		public boolean tryLock(String label) {
			boolean result = tryLock();
			if (result) {
				if (log.isLoggable(Level.FINEST) ) {
					StringBuilder lockLog = new StringBuilder(Thread.currentThread().getName())
							.append(": ");
					for (int i = 0; i < labels.size(); i++) lockLog.append("  ");
					lockLog.append(label).append(" locked");
					log.finest(lockLog.toString());
				}
				labels.add(label);
			} else {
				if (log.isLoggable(Level.INFO) ) {
					log.info(Thread.currentThread().getName() + ": failed to lock " + label);
				}
			}
			return result;
		}

		@Override
		public void unlock() {
			String label = labels.remove(labels.size() - 1);
			if (log.isLoggable(Level.FINEST) ) {
				StringBuilder lockLog = new StringBuilder(Thread.currentThread().getName())
						.append(": ");
				for (int i = 0; i < labels.size(); i++) lockLog.append("  ");
				lockLog.append(label).append(" unlocked");
				log.finest(lockLog.toString());
			}
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