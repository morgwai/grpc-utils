// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.*;



/**
 * A fake {@link CallStreamObserver} testing helper class.
 * Helps to emulate behavior of an inbound and the gRPC system.
 * <p>
 * <b>Note:</b> in most cases it is better to use {@link io.grpc.inprocess.InProcessChannelBuilder}
 * for testing gRPC methods. This class is mainly intended for testing infrastructure parts.</p>
 * <p>
 * Usage:</p>
 * <ol>
 *   <li>Configure observer's readiness by adjusting {@link #outputBufferSize} and
 *     {@link #unreadyDurationMillis} variables.</li>
 *   <li>Depending on client type (unary/streaming) of your gRPC method pass it to one of
 *     {@link #callWithinListenerLock(Consumer)},
 *     {@link #callWithinListenerLock(Function, Consumer)} methods.</li>
 *   <li>{@link #awaitFinalization(long)} can be used to wait until {@link #onCompleted()} or
 *     {@link #onError(Throwable)} is called.</li>
 *   <li>Client canceling can be simulated using {@link #simulateCancel()} method.</li>
 *   <li>Results can be verified with {@link #getOutputData()}, {@link #isFinalized()},
 *     {@link #getReportedError()} methods and by shutting down and inspecting
 *     {@link LoggingExecutor} supplied to the constructor.</li>
 * </ol>
 */
public class FakeOutboundObserver<OutboundT, ControlT>
		extends CallStreamObserver<OutboundT> {



	/**
	 * @param grpcInternalExecutor executor for gRPC internal tasks, such as marking response
	 * observer as ready, delivering requested messages etc. Its pool size should be not smaller
	 * than the number of requests concurrently processed by the code under test (usually determined
	 * by the argument of the initial call to {@link ServerCallStreamObserver#request(int)}.
	 */
	public FakeOutboundObserver(LoggingExecutor grpcInternalExecutor) {
		this.grpcInternalExecutor = grpcInternalExecutor;
	}

	final LoggingExecutor grpcInternalExecutor;



	/**
	 * List of arguments of calls to {@link #onNext(Object)}.
	 */
	public List<OutboundT> getOutputData() { return outputData; }
	final List<OutboundT> outputData = new LinkedList<>();

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
	public void callWithinListenerLock(Consumer<StreamObserver<OutboundT>> unaryClientMethod) {
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
			Function<StreamObserver<OutboundT>, StreamObserver<RequestT>> streamingClientMethod,
			Consumer<StreamObserver<RequestT>> requestProducer) {
		StreamObserver<RequestT> requestObserver;
		synchronized (listenerLock) {
			requestObserver = streamingClientMethod.apply(this);
		}
		startMessageDelivery(requestObserver, requestProducer);
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
	public void onNext(OutboundT message) {
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
				ready = false;
				log.fine("marked response observer unready");
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
		} catch (InterruptedException ignored) {}
		synchronized (listenerLock) {
			if ( ! ready) {
				log.fine("marking response observer ready again");
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



	public boolean isFinalized() { return finalized; }
	boolean finalized = false;
	final CountDownLatch finalizationGuard = new CountDownLatch(1);



	@Override
	public void onCompleted() {
		if ( ! concurrencyGuard.tryLock("onCompleted")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			log.fine("response completed");
			synchronized (finalizationGuard) {
				if (finalized) throw new IllegalStateException("multiple finalizations");
				finalized = true;
			}
		} finally {
			finalizationGuard.countDown();
			concurrencyGuard.unlock();
		}
	}



	@Override
	public void onError(Throwable t) {
		reportedError = t;
		onCompleted();
	}

	/**
	 * Stored argument of {@link #onError(Throwable)}.
	 */
	public Throwable getReportedError() { return reportedError; }
	Throwable reportedError;



	public void cancel(String message, Throwable reason) {
		cancelMessage = message;
		onError(reason);
	}

	public String getCancelMessage() { return cancelMessage; }
	String cancelMessage;



	/**
	 * Awaits until finalization (call to either {@link #onCompleted()} or
	 * {@link #onError(Throwable)} or {@link #simulateCancel()} ) occurs or {@code timeoutMillis}
	 * passes.
	 * @throws RuntimeException if {@code timeoutMillis} is exceeded.
	 */
	public void awaitFinalization(long timeoutMillis) throws InterruptedException {
		finalizationGuard.await(timeoutMillis, TimeUnit.MILLISECONDS);
		synchronized (finalizationGuard) {
			if (!finalized && reportedError == null) {
				throw new RuntimeException("timeout awaiting for finalization");
			}
		}
	}



	public static long getRemainingMillis(long startMillis, long timeoutMillis) {
		return Math.max(1l, timeoutMillis + startMillis - System.currentTimeMillis());
	}



	@Override
	public void disableAutoInboundFlowControl() {
		if ( ! concurrencyGuard.tryLock("disableAutoRequest")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.autoRequest = false;
		} finally {
			concurrencyGuard.unlock();
		}
	}

	boolean autoRequest = true;



	/**
	 * todo: update
	 * Sets up delivery of request messages from {@code inboundMessageProducer} to
	 * {@code inboundObserver} (test subject) whenever {@link #request(int)} method is called.<br/>
	 * Also delivers messages for all accumulated {@link #request(int)} calls that happened before
	 * this method was called.
	 * <p>
	 * Note: this method is exposed for low level testing of stand-alone request observers. It is
	 * automatically called by {@link #callWithinListenerLock(Function, Consumer)} which should be
	 * used for normal gRPC method testing.</p>
	 */
	@SuppressWarnings("unchecked")
	<InboundT> void startMessageDelivery(
		StreamObserver<InboundT> inboundObserver,
		Consumer<StreamObserver<InboundT>> inboundMessageProducer
	) {
		// call beforeStart(...) if needed
		final var concurrentInboundObserver =
				(ConcurrentInboundObserver<InboundT, OutboundT, ControlT>) inboundObserver;
		if (concurrentInboundObserver.onBeforeStartHandler != null) {
			concurrentInboundObserver.beforeStart(this.asClientCallControlObserver());
		}

		// initial onReady() callback
		if (onReadyHandler != null) {
			synchronized (listenerLock) {
				log.fine("delivering initial onReady() callback");
				if (onReadyHandler != null) onReadyHandler.run();
			}
		}

		this.requestProducer = (Consumer<StreamObserver<?>>)(Consumer<?>) inboundMessageProducer;
		this.requestObserver = new StreamObserver<InboundT>() {

			@Override
			public void onNext(InboundT message) {
				synchronized(listenerLock) {
					inboundObserver.onNext(message);
				}
			}

			@Override
			public void onError(Throwable error) {
				synchronized(listenerLock) {
					inboundObserver.onError(error);
				}
			}

			@Override
			public void onCompleted() {
				synchronized(listenerLock) {
					inboundObserver.onCompleted();
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
						if ( !finalized) requestOne();
					}
				}
			}

			@Override public String toString() {
				return "requestProducer " + requestProducer.toString();
			}
		});
	}



	/**
	 * Simulates canceling the call by the client side.
	 */
	public void simulateCancel() {
		cancelled = true;
		synchronized (listenerLock) {
			if (onCancelHandler != null) onCancelHandler.run();
		}
	}

	public boolean isCancelled() {
		return cancelled;
	}

	volatile boolean cancelled = false;

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



	ServerCallStreamObserver<OutboundT> asServerCallResponseObserver() {
		return new ServerCallStreamObserver<>() {
			// ServerCallStreamObserver
			@Override public boolean isCancelled() {return FakeOutboundObserver.this.isCancelled();}
			@Override public void setOnCancelHandler(Runnable onCancelHandler) {
				FakeOutboundObserver.this.setOnCancelHandler(onCancelHandler);
			}
			@Override public void setOnCloseHandler(Runnable onCloseHandler) {}
			@Override public void disableAutoRequest() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void setCompression(String compression) {
				FakeOutboundObserver.this.setCompression(compression);
			}

			// CallStreamObserver
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
			@Override public void disableAutoInboundFlowControl() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				FakeOutboundObserver.this.setOnReadyHandler(onReadyHandler);
			}
			@Override public boolean isReady() { return FakeOutboundObserver.this.isReady(); }
			@Override public void request(int count) { FakeOutboundObserver.this.request(count); }
			@Override public void onNext(OutboundT value) {FakeOutboundObserver.this.onNext(value);}
			@Override public void onError(Throwable t) { FakeOutboundObserver.this.onError(t); }
			@Override public void onCompleted() { FakeOutboundObserver.this.onCompleted(); }
		};
	}

	ClientCallStreamObserver<OutboundT> asClientCallRequestObserver() {
		return new ClientCallStreamObserver<>() {
			// ClientCallStreamObserver
			@Override public void cancel(@Nullable String message, @Nullable Throwable cause) {
				FakeOutboundObserver.this.cancel(message, cause);
			}
			@Override public void disableAutoRequestWithInitial(int request) {
				throw new IllegalArgumentException(
					"attempted inbound control on outbound ClientCallRequestObserver");
			}

			// CallStreamObserver
			@Override public void request(int count) {
				throw new IllegalArgumentException(
					"attempted inbound control on outbound ClientCallRequestObserver");
			}
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
			@Override public void disableAutoInboundFlowControl() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				FakeOutboundObserver.this.setOnReadyHandler(onReadyHandler);
			}
			@Override public boolean isReady() { return FakeOutboundObserver.this.isReady(); }
			@Override public void onNext(OutboundT value) {FakeOutboundObserver.this.onNext(value);}
			@Override public void onError(Throwable t) { FakeOutboundObserver.this.onError(t); }
			@Override public void onCompleted() { FakeOutboundObserver.this.onCompleted(); }
		};
	}

	ClientCallStreamObserver<ControlT> asClientCallControlObserver() {
		return new ClientCallStreamObserver<>() {
			// ClientCallStreamObserver
			@Override public void cancel(@Nullable String message, @Nullable Throwable cause) {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public void disableAutoRequestWithInitial(int request) {
				disableAutoInboundFlowControl();
				request(request);
			}

			// CallStreamObserver
			@Override public void request(int count) {
				FakeOutboundObserver.this.request(count);
			}
			@Override public void disableAutoInboundFlowControl() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void setMessageCompression(boolean enable) {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public boolean isReady() {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public void onNext(ControlT value) {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public void onError(Throwable t) {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
			@Override public void onCompleted() {
				throw new IllegalArgumentException(
					"attempted output to inbound control ClientCallRequestObserver");
			}
		};
	}



	/**
	 * Logs task scheduling and executions and scheduling rejections.
	 */
	public static class LoggingExecutor extends ThreadPoolExecutor {

		/**
		 * List of all rejected tasks.
		 */
		public List<Runnable> getRejectedTasks() { return rejectedTasks; }
		List<Runnable> rejectedTasks = new LinkedList<>();

		public String getName() { return name; }
		final String name;



		public LoggingExecutor(String name, int poolSize) {
			super(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
			this.name = name;
			setRejectedExecutionHandler((task, executor) -> {
				log.log(Level.SEVERE, name + " rejected " + task, new Exception());
				rejectedTasks.add(task);
			});
		}



		@Override
		public void execute(Runnable task) {
			final int taskId = taskIdSequence.incrementAndGet();
			if (log.isLoggable(Level.FINER)) {
				log.finer(name + " scheduling " + taskId + ": " + task);
			}
			super.execute(new Runnable() {

				@Override public void run() {
					if (log.isLoggable(Level.FINER)) {
						log.finer(name + " starting " + taskId + ": " + task);
					}
					task.run();
					if (log.isLoggable(Level.FINER)) {
						log.finer(name + " completed " + taskId + ": " + task);
					}
				}

				@Override public String toString() { return "" + taskId + ": " + task; }
			});
		}

		final AtomicInteger taskIdSequence = new AtomicInteger(0);



		@Override
		public void shutdown() {
			log.fine(name + " shutting down");
			super.shutdown();
		}



		@Override
		public List<Runnable> shutdownNow() {
			log.severe(name + " shutting down forcibly");
			return super.shutdownNow();
		}



		public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
			return super.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
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
	 * <code>FINER</code> will log every message sent to the observer and every task dispatched
	 * to {@link LoggingExecutor}.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static final Logger log = Logger.getLogger(FakeOutboundObserver.class.getName());
	public static Logger getLogger() { return log; }
}
