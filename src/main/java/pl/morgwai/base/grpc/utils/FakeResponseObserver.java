// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;



/**
 * Testing helper class. Helps to emulate behavior of a client and the gRPC system during server
 * side tests.
 */
public class FakeResponseObserver<ResponseT>
		extends ServerCallStreamObserver<ResponseT> {



	/**
	 * @param grpcInternalExecutor executor for gRPC internal tasks, such as marking response
	 * observer as ready, delivering requested messages etc.
	 */
	public FakeResponseObserver(Executor grpcInternalExecutor) {
		this.grpcInternalExecutor = grpcInternalExecutor;
	}

	Executor grpcInternalExecutor;

	/**
	 * {@code true} if there were attempts to execute more tasks after {@link #grpcInternalExecutor}
	 * was shutdown.
	 */
	public boolean getExecuteAfterShutdown() { return executeAfterShutdown; }
	volatile boolean executeAfterShutdown = false;



	/**
	 * Verifies that at most 1 thread calls this observer's methods concurrently.
	 */
	LoggingReentrantLock concurrencyGuard = new LoggingReentrantLock();

	/**
	 * This lock ensures that user's request observer will be called by at most 1 thread
	 * concurrently, just as gRPC listener does. It is exposed for cases when user code simulates
	 * gRPC listener behavior, usually when triggering a test.
	 */
	public Object getListenerLock() { return listenerLock; }
	Object listenerLock = new Object();



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
				try {
					grpcInternalExecutor.execute(() -> markObserverReady(unreadyDurationMillis));
				} catch (Exception e) {
					executeAfterShutdown = true;
				}
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
	 * Dispatched to {@link #grpcInternalExecutor} by {@link #request(int)} method.<br/>
	 * It should usually call <code>requestObserver</code>'s {@link StreamObserver#onNext(Object)}
	 * or {@link StreamObserver#onCompleted()} to simulate a client delivering request messages.
	 * <br/>
	 * Lambda instances are usually created in test methods to simulate specific client behavior.
	 */
	public Runnable messageProducer;

	@Override
	public void request(int count) {
		if ( ! autoRequestDisabled) throw new AssertionError("autoRequest was not disabled");
		if (messageProducer != null) {
			try {
				for (int i = 0; i < count; i++) grpcInternalExecutor.execute(messageProducer);
			} catch (Exception e) {
				executeAfterShutdown = true;
			}
		}
	}



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