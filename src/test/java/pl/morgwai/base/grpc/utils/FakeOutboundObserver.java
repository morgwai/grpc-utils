// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.stub.*;
import pl.morgwai.base.utils.concurrent.NamingThreadFactory;
import pl.morgwai.base.utils.concurrent.TaskTrackingThreadPoolExecutor;

import static org.junit.Assert.*;



/**
 * A fake {@link CallStreamObserver} testing helper class.
 * Helps to emulate behavior of an inbound and the gRPC system. ejona86 hates me.
 */
public class FakeOutboundObserver<InboundT, OutboundT> extends ServerCallStreamObserver<OutboundT> {



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
	 * Ensures listener concurrency contract: user inbound observer may be called concurrently by at
	 * most 1 thread
	 */
	private final Object listenerLock = new Object();

	/** Verifies that at most 1 thread concurrently calls this observer's methods. */
	private final LoggingReentrantLock concurrencyGuard = new LoggingReentrantLock();



	/** For low level testing of onReady and onCancel handlers. */
	public void runWithinListenerLock(Runnable handler) {
		synchronized (listenerLock) {
			handler.run();
		}
	}



	/////////////////////////////
	// output and readiness stuff
	/////////////////////////////



	/**
	 * Response observer becomes unready after each <code>outputBufferSize</code> messages are
	 * submitted to it. Default is <code>0</code> which means always ready.
	 */
	public volatile int outputBufferSize = 0;

	/** Duration for which observer will be unready. By default 1ms. */
	public volatile long unreadyDurationMillis = 1L;

	/** List of arguments of calls to {@link #onNext(Object)}. */
	public List<OutboundT> getOutputData() {
		if ( !isFinalized()) {
			throw new IllegalStateException("observer not yet finalized");
		}
		return outputData;
	}
	private final List<OutboundT> outputData = new LinkedList<>();

	/**
	 * Number of messages that were submitted to this observer after
	 * {@link #isFinalized() finalization}.
	 */
	public int getMessagesAfterFinalizationCount() { return messagesAfterFinalizationCount; }
	private int messagesAfterFinalizationCount = 0;

	private Runnable onReadyHandler;
	private volatile boolean ready = true;



	@Override
	public void onNext(OutboundT message) {
		if ( !concurrencyGuard.tryLock("onNext")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			if (finalized) {
				messagesAfterFinalizationCount++;
				throw new IllegalStateException("already finalized");
			}

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
				grpcInternalExecutor.execute(new ReadyMarker(unreadyDurationMillis));
			}
		} finally {
			concurrencyGuard.unlock();
		}
	}

	/** sleep {@link #delayMillis}, mark observer ready, run onReadyHandler */
	private class ReadyMarker implements Runnable {

		final long delayMillis;

		private ReadyMarker(long delayMillis) { this.delayMillis = delayMillis; }

		@Override public void run() {
			if (delayMillis > 0L) try {
				Thread.sleep(delayMillis);
			} catch (InterruptedException ignored) {}
			synchronized (listenerLock) {
				if ( !ready) {
					log.fine("marking response observer ready again");
					ready = true;
					if (onReadyHandler != null) onReadyHandler.run();
				}
			}
		}

		@Override public String toString() {
			return "ReadyMarker";
		}
	}



	@Override
	public boolean isReady() {
		if ( !concurrencyGuard.tryLock("isReady")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			return ready;
		} finally {
			concurrencyGuard.unlock();
		}
	}



	@Override
	public void setOnReadyHandler(Runnable onReadyHandler) {
		if ( !concurrencyGuard.tryLock("setOnReadyHandler")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.onReadyHandler = onReadyHandler;
		} finally {
			concurrencyGuard.unlock();
		}
	}



	/////////////////////
	// finalization stuff
	/////////////////////



	private final CountDownLatch finalizationGuard = new CountDownLatch(1);

	/** Whether {@link #onCompleted()} or {@link #onError(Throwable)} was called. */
	public boolean isFinalized() { return finalized; }
	private boolean finalized;

	/** Error reported via {@link #onError(Throwable)}. */
	public Throwable getReportedError() { return reportedError; }
	private Throwable reportedError;

	/**
	 * Message reported via {@link ClientCallStreamObserver#cancel(String, Throwable)} called on
	 * observer passed {@link #asClientOutboundObserver()}.
	 */
	public String getCancelMessage() { return cancelMessage; }
	private String cancelMessage;

	/**
	 * Reason reported via {@link ClientCallStreamObserver#cancel(String, Throwable)} called on
	 * observer passed {@link #asClientOutboundObserver()}.
	 */
	public Throwable getCancelReason() { return cancelReason; }
	private Throwable cancelReason;
	/**
	 *  How many bogus additional calls to either {@link #onCompleted()} or
	 *  {@link #onError(Throwable)} there were apart from the first expected one.
	 */
	public int getExtraFinalizationCount() { return extraFinalizationCount; }
	private int extraFinalizationCount = 0;



	@Override
	public void onCompleted() {
		if ( !concurrencyGuard.tryLock("onCompleted")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			log.fine("onCompleted()");
			synchronized (finalizationGuard) {
				if (finalized) {
					extraFinalizationCount++;
					throw new IllegalStateException("multiple finalizations");
				}
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



	/** For {@link #asClientOutboundObserver()} perspective. */
	private void cancel(String message, Throwable reason) {
		cancelMessage = message;
		cancelReason = reason;
		onCompleted();
	}



	/**
	 * Awaits until finalization (call to either {@link #onCompleted()} or
	 * {@link #onError(Throwable)} or {@link #simulateCancel()} ) occurs or {@code timeoutMillis}
	 * passes.
	 * @return {@code true} if this observer is finalized properly, {@code false} if
	 * {@code timeoutMillis} exceeds.
	 */
	public boolean awaitFinalization(long timeoutMillis) throws InterruptedException {
		return awaitFinalization(timeoutMillis, TimeUnit.MILLISECONDS);
	}



	public boolean awaitFinalization(long timeout, TimeUnit unit) throws InterruptedException {
		finalizationGuard.await(timeout, unit);
		synchronized (finalizationGuard) {
			if ( !finalized && reportedError == null) return false;
		}
		return true;
	}



	/////////////////////////////////
	// inbound message delivery stuff
	/////////////////////////////////



	private volatile Consumer<StreamObserver<InboundT>> inboundMessageProducer;
	private StreamObserver<InboundT> inboundObserver;
	private int accumulatedMessageRequestCount = 0;
	private boolean autoRequest = true;



	/**
	 * Sets up delivery of inbound messages from {@code inboundMessageProducer} to
	 * {@code inboundObserver} (test subject), delivers the initial call to {@link #onReadyHandler}.
	 * Next, delivers messages for all accumulated {@link #request(int)} calls that happened
	 * before this method was called.
	 */
	public void startServerMessageDelivery(
		StreamObserver<InboundT> inboundObserver,
		Consumer<StreamObserver<InboundT>> inboundMessageProducer
	) {
		startMessageDelivery(inboundObserver, inboundMessageProducer, () -> {});
	}

	/**
	 * Same as {@link #startServerMessageDelivery(StreamObserver, Consumer)} but also calls
	 * {@link ClientResponseObserver#beforeStart(ClientCallStreamObserver)}.
	 */
	public void startClientMessageDelivery(
		ClientResponseObserver<?, InboundT> inboundObserver,
		Consumer<StreamObserver<InboundT>> inboundMessageProducer
	) {
		startMessageDelivery(
			inboundObserver,
			inboundMessageProducer,
			() -> {
				log.fine("calling beforeStart(...)");
				inboundObserver.beforeStart(
					FakeOutboundObserver.this.asClientInboundControlObserver());
			}
		);
	}

	private void startMessageDelivery(
		StreamObserver<InboundT> inboundObserver,
		Consumer<StreamObserver<InboundT>> inboundMessageProducer,
		Runnable callBeforeStart
	) {
		this.inboundMessageProducer = inboundMessageProducer;
		this.inboundObserver = new StreamObserver<>() {

			@Override public void onNext(InboundT message) {
				synchronized(listenerLock) {
					inboundObserver.onNext(message);
				}
			}

			@Override public void onError(Throwable error) {
				synchronized(listenerLock) {
					inboundObserver.onError(error);
				}
			}

			@Override public void onCompleted() {
				synchronized(listenerLock) {
					inboundObserver.onCompleted();
				}
			}
		};

		// dispatch beforeStart(...) + initial onReady() + delivery-dispatch
		grpcInternalExecutor.execute(new Runnable() {

			@Override public void run() {
				synchronized (listenerLock) {
					callBeforeStart.run();

					// initial onReady()
					if (onReadyHandler != null) {
						log.fine("initial onReady() call");
						onReadyHandler.run();
					}
				}

				// delivery-dispatch
				request(accumulatedMessageRequestCount);
			}

			@Override public String toString() {
				return "beforeStart(...) + initial onReady() + delivery-dispatch";
			}
		});
	}



	@Override
	public void disableAutoInboundFlowControl() {
		if ( !concurrencyGuard.tryLock("disableAutoRequest")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.autoRequest = false;
		} finally {
			concurrencyGuard.unlock();
		}
	}



	@Override
	public void request(int count) {
		if (autoRequest) throw new AssertionError("autoRequest was not disabled");
		if (inboundMessageProducer == null) {
			accumulatedMessageRequestCount += count;
			return;
		}
		for (int i = 0; i < count; i++) requestOne();
	}

	void requestOne() {
		grpcInternalExecutor.execute(new Runnable() {

			@Override public void run() {
				inboundMessageProducer.accept(inboundObserver);
				if (autoRequest) {
					synchronized (finalizationGuard) {
						if ( !finalized) requestOne();
					}
				}
			}

			@Override public String toString() {
				return "requestProducer " + inboundMessageProducer.toString();
			}
		});
	}



	///////////////////
	// cancelling stuff
	///////////////////




	public boolean isCancelled() {
		return cancelled;
	}
	private volatile boolean cancelled = false;

	private Runnable onCancelHandler;



	/** Simulates canceling the call by the client side. */
	public void simulateCancel() {
		cancelled = true;
		synchronized (listenerLock) {
			if (onCancelHandler != null) onCancelHandler.run();
		}
	}



	@Override
	public void setOnCancelHandler(Runnable onCancelHandler) {
		if ( !concurrencyGuard.tryLock("setOnCancelHandler")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			this.onCancelHandler = onCancelHandler;
		} finally {
			concurrencyGuard.unlock();
		}
	}



	//////////////////////
	// interface leftovers
	//////////////////////



	@Override
	public void setCompression(String compression) {
		if ( !concurrencyGuard.tryLock("setCompression")) {
			throw new AssertionError("concurrency violation");
		}
		concurrencyGuard.unlock();
	}



	@Override
	public void setMessageCompression(boolean enable) {
		if ( !concurrencyGuard.tryLock("setMessageCompression")) {
			throw new AssertionError("concurrency violation");
		}
		concurrencyGuard.unlock();
	}



	@Override
	public void setOnCloseHandler(Runnable onCloseHandler) {
		if ( !concurrencyGuard.tryLock("setMessageCompression")) {
			throw new AssertionError("concurrency violation");
		}
		try {
			super.setOnCloseHandler(onCloseHandler);
		} finally {
			concurrencyGuard.unlock();
		}
	}



	//////////////////////////////////////////
	// perspectives for various types of tests
	//////////////////////////////////////////



	/** For {@link NestedClientResponseObserverTests}. */
	ServerCallStreamObserver<OutboundT> asServerOutboundObserver() {
		return new ServerCallStreamObserver<>() {
			// enabled outbound
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				FakeOutboundObserver.this.setOnReadyHandler(onReadyHandler);
			}
			@Override public boolean isReady() {
				return FakeOutboundObserver.this.isReady();
			}
			@Override public void onNext(OutboundT value) {
				FakeOutboundObserver.this.onNext(value);
			}
			@Override public void onError(Throwable t) {
				FakeOutboundObserver.this.onError(t);
			}
			@Override public void onCompleted() {
				FakeOutboundObserver.this.onCompleted();
			}

			// disabled inbound control
			@Override public void request(int count) {
				throw new IllegalArgumentException(
						"attempted inbound control on outbound only ServerObserver");
				}
			@Override public void disableAutoInboundFlowControl() {
				throw new IllegalArgumentException(
						"attempted inbound control on outbound only ServerObserver");
			}
			@Override public void disableAutoRequest() {
				throw new IllegalArgumentException(
						"attempted inbound control on outbound only ServerObserver");
			}

			// leftovers
			@Override public boolean isCancelled() {
				return FakeOutboundObserver.this.isCancelled();
			}
			@Override public void setOnCancelHandler(Runnable onCancelHandler) {
				FakeOutboundObserver.this.setOnCancelHandler(onCancelHandler);
			}
			@Override public void setOnCloseHandler(Runnable onCloseHandler) {
				FakeOutboundObserver.this.setOnCloseHandler(onCloseHandler);
			}
			@Override public void setCompression(String compression) {
				FakeOutboundObserver.this.setCompression(compression);
			}
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
		};
	}



	/** For {@link ServerRequestObserverTests}. */
	ServerCallStreamObserver<OutboundT> asServerInboundControlObserver() {
		return new ServerCallStreamObserver<>() {
			// enabled inbound control
			@Override public void request(int count){
				FakeOutboundObserver.this.request(count);
			}
			@Override public void disableAutoInboundFlowControl() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void disableAutoRequest() {
				FakeOutboundObserver.this.disableAutoRequest();
			}

			// disabled outbound
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				throw new IllegalArgumentException(
						"attempted output to inbound control only ServerObserver");
			}
			@Override public boolean isReady() {
				throw new IllegalArgumentException(
						"attempted output to inbound control only ServerObserver");
			}
			@Override public void onNext(OutboundT value) {
				throw new IllegalArgumentException(
						"attempted output to inbound control only ServerObserver");
			}
			@Override public void onError(Throwable t) {
				throw new IllegalArgumentException(
						"attempted output to inbound control only ServerObserver");
			}
			@Override public void onCompleted() {
				throw new IllegalArgumentException(
						"attempted output to inbound control only ServerObserver");
			}

			// leftovers
			@Override public boolean isCancelled() {
				return FakeOutboundObserver.this.isCancelled();
			}
			@Override public void setOnCancelHandler(Runnable onCancelHandler) {
				FakeOutboundObserver.this.setOnCancelHandler(onCancelHandler);
			}
			@Override public void setOnCloseHandler(Runnable onCloseHandler) {
				FakeOutboundObserver.this.setOnCloseHandler(onCloseHandler);
			}
			@Override public void setCompression(String compression) {
				FakeOutboundObserver.this.setCompression(compression);
			}
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
		};
	}



	/** For {@link ServerRequestObserverTests} and {@link ChainedClientResponseObserverTests} */
	ClientCallStreamObserver<OutboundT> asClientOutboundObserver() {
		return new ClientCallStreamObserver<>() {
			// cancel
			@Override public void cancel(@Nullable String message, @Nullable Throwable cause) {
				FakeOutboundObserver.this.cancel(message, cause);
			}

			// enabled outbound
			@Override public void setOnReadyHandler(Runnable onReadyHandler) {
				FakeOutboundObserver.this.setOnReadyHandler(onReadyHandler);
			}
			@Override public boolean isReady() {
				return FakeOutboundObserver.this.isReady();
			}
			@Override public void onNext(OutboundT value) {
				FakeOutboundObserver.this.onNext(value);
			}
			@Override public void onError(Throwable t) {
				FakeOutboundObserver.this.onError(t);
			}
			@Override public void onCompleted() {
				FakeOutboundObserver.this.onCompleted();
			}

			// disabled inbound control
			@Override public void request(int count) {
				throw new IllegalArgumentException(
					"attempted inbound control on outbound ClientCallRequestObserver");
			}
			@Override public void disableAutoInboundFlowControl() {
				throw new IllegalArgumentException(
						"attempted inbound control on outbound ClientCallRequestObserver");
			}
			@Override public void disableAutoRequestWithInitial(int request) {
				throw new IllegalArgumentException(
						"attempted inbound control on outbound ClientCallRequestObserver");
			}

			// leftovers
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
		};
	}



	/** For {@link #startClientMessageDelivery(ClientResponseObserver, Consumer)}. */
	<ControlT> ClientCallStreamObserver<ControlT> asClientInboundControlObserver() {
		return new ClientCallStreamObserver<>() {
			// cancel
			@Override public void cancel(@Nullable String message, @Nullable Throwable cause) {
				grpcInternalExecutor.execute(() -> {
					synchronized (listenerLock) {
						inboundObserver.onError(Status.CANCELLED.asRuntimeException());
					}
				});
			}

			// enabled inbound control
			@Override public void request(int count) {
				FakeOutboundObserver.this.request(count);
			}
			@Override public void disableAutoInboundFlowControl() {
				FakeOutboundObserver.this.disableAutoInboundFlowControl();
			}
			@Override public void disableAutoRequestWithInitial(int request) {
				disableAutoInboundFlowControl();
				request(request);
			}

			// disabled outbound
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

			// leftovers
			@Override public void setMessageCompression(boolean enable) {
				FakeOutboundObserver.this.setMessageCompression(enable);
			}
		};
	}



	/////////////////////////////
	// nested test helper classes
	/////////////////////////////



	/** Logs task scheduling and executions, scheduling rejections and uncaught exceptions. */
	public static class LoggingExecutor extends TaskTrackingThreadPoolExecutor {

		/** List of all rejected tasks. */
		public List<Runnable> getRejectedTasks() { return rejectedTasks; }
		final List<Runnable> rejectedTasks = new LinkedList<>();

		/** Uncaught exceptions mapped to tasks that threw them. */
		public Map<Throwable, Runnable> getUncaughtTaskExceptions() {return uncaughtTaskExceptions;}
		final Map<Throwable, Runnable> uncaughtTaskExceptions = new HashMap<>();

		public String getName() { return name; }
		final String name;



		public LoggingExecutor(String name, int poolSize) {
			super(poolSize, poolSize, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
					new NamingThreadFactory(name));
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
					try {
						task.run();
					} catch (Throwable t) {
						uncaughtTaskExceptions.put(t, task);
						if (t instanceof Error) throw t;
					} finally {
						if (log.isLoggable(Level.FINER)) {
							log.finer(name + " completed " + taskId + ": " + task);
						}
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



		public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
			return super.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
		}



		public void verify(Throwable... expectedUncaught) {
			assertTrue("no task scheduling failures should occur on " + getName(),
					getRejectedTasks().isEmpty());
			assertEquals("only expected exceptions should be thrown by tasks",
					expectedUncaught.length, getUncaughtTaskExceptions().size());
			for (var exception: expectedUncaught) {
				assertTrue("all expected exceptions should be thrown by tasks",
						getUncaughtTaskExceptions().containsKey(exception));
			}
			if (isTerminated()) return;
			for (var stuckTask: getRunningTasks()) {
				log.severe(getName() + ": stuck " + stuckTask);
			}
			for (var unexecutedTask: shutdownNow()) {
				log.severe(getName() + ": unexecuted " + unexecutedTask);
			}
			fail(getName() + " should shutdown cleanly");
		}
	}



	static class LoggingReentrantLock extends ReentrantLock {

		final List<String> labels = new LinkedList<>();

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



	///////////////////
	// logging settings
	///////////////////



	/**
	 * <code>FINE</code> will log finalizing events and marking observer ready/unready.<br/>
	 * <code>FINER</code> will log every message sent to the observer and every task dispatched
	 * to {@link LoggingExecutor}.<br/>
	 * <code>FINEST</code> will log concurrency debug info.
	 */
	static final Logger log = Logger.getLogger(FakeOutboundObserver.class.getName());
	public static Logger getLogger() { return log; }
}
