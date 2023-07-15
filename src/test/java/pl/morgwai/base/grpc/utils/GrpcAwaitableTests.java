// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.*;

import io.grpc.*;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;



public class GrpcAwaitableTests {
	// This is almost exact double copy-paste of Awaitable.ofXXX test methods from java-utils.
	// Unfortunately while Servers, Channels and Executor have methods related to shutting down
	// with almost the same signatures, they don't share any interface.



	@Test
	public void testAwaitableOfServerTermination() throws InterruptedException {
		final var executor = new ThreadPoolExecutor(
				2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
		final var server = new ExecutorServer(executor);
		final var latch = new CountDownLatch(1);
		executor.execute(
			() -> {
				try {
					latch.await();
				} catch (InterruptedException ignored) {}
			}
		);
		final var termination = GrpcAwaitable.ofTermination(server);
		assertFalse("server should not be shutdown until termination is being awaited",
				server.isShutdown());
		assertFalse("termination should fail before latch is lowered", termination.await(20L));
		assertTrue("server should be shutdown", server.isShutdown());
		assertFalse("termination should fail before latch is lowered", server.isTerminated());
		latch.countDown();
		assertTrue("termination should succeed after latch is lowered", termination.await(20L));
		assertTrue("termination should succeed after latch is lowered", server.isTerminated());
	}



	@Test
	public void testAwaitableOfServerEnforcedTermination() throws InterruptedException {
		final var executor = new ThreadPoolExecutor(
				2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
		final var server = new ExecutorServer(executor);
		final var latch = new CountDownLatch(1);
		executor.execute(
			() -> {
				try {
					Thread.sleep(100_000L);
				} catch (InterruptedException e) {
					try {
						latch.await();
					} catch (InterruptedException ignored) {}
				}
			}
		);
		final var enforcedTermination = GrpcAwaitable.ofEnforcedTermination(server);
		assertFalse("server should not be shutdown until termination is being awaited",
				server.isShutdown());
		assertFalse("termination should fail", enforcedTermination.await(20L));
		assertFalse("termination should fail", server.isTerminated());
		assertTrue("server should be shutdown", server.isShutdown());
		latch.countDown();
		assertTrue("finally server should terminate successfully",
				server.awaitTermination(50L, TimeUnit.MILLISECONDS));
	}



	@Test
	public void testAwaitableOfChannelTermination() throws InterruptedException {
		final var executor = new ThreadPoolExecutor(
				2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
		final var channel = new ExecutorChannel(executor);
		final var latch = new CountDownLatch(1);
		executor.execute(
			() -> {
				try {
					latch.await();
				} catch (InterruptedException ignored) {}
			}
		);
		final var termination = GrpcAwaitable.ofTermination(channel);
		assertFalse("channel should not be shutdown until termination is being awaited",
				channel.isShutdown());
		assertFalse("termination should fail before latch is lowered", termination.await(20L));
		assertTrue("channel should be shutdown", channel.isShutdown());
		assertFalse("termination should fail before latch is lowered", channel.isTerminated());
		latch.countDown();
		assertTrue("termination should succeed after latch is lowered", termination.await(20L));
		assertTrue("termination should succeed after latch is lowered", channel.isTerminated());
	}



	@Test
	public void testAwaitableOfChannelEnforcedTermination() throws InterruptedException {
		final var executor = new ThreadPoolExecutor(
				2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
		final var channel = new ExecutorChannel(executor);
		final var latch = new CountDownLatch(1);
		executor.execute(
			() -> {
				try {
					Thread.sleep(100_000L);
				} catch (InterruptedException e) {
					try {
						latch.await();
					} catch (InterruptedException ignored) {}
				}
			}
		);
		final var enforcedTermination = GrpcAwaitable.ofEnforcedTermination(channel);
		assertFalse("channel should not be shutdown until termination is being awaited",
				channel.isShutdown());
		assertFalse("termination should fail", enforcedTermination.await(20L));
		assertFalse("termination should fail", channel.isTerminated());
		assertTrue("channel should be shutdown", channel.isShutdown());
		latch.countDown();
		assertTrue("finally channel should terminate successfully",
				channel.awaitTermination(50L, TimeUnit.MILLISECONDS));
	}



	static class ExecutorServer extends Server {

		final ExecutorService executor;
		ExecutorServer(ExecutorService executor) { this.executor = executor; }

		@Override public Server start() { return this; }

		@Override public Server shutdown() {
			executor.shutdown();
			return this;
		}

		@Override public Server shutdownNow() {
			executor.shutdownNow();
			return this;
		}

		@Override public boolean isShutdown() {
			return executor.isShutdown();
		}

		@Override public boolean isTerminated() {
			return executor.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return executor.awaitTermination(timeout, unit);
		}

		@Override public void awaitTermination() throws InterruptedException {
			while ( !executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS));
		}
	}



	static class ExecutorChannel extends ManagedChannel {

		final ExecutorService executor;
		ExecutorChannel(ExecutorService executor) { this.executor = executor; }

		@Override public ExecutorChannel shutdown() {
			executor.shutdown();
			return this;
		}

		@Override public ExecutorChannel shutdownNow() {
			executor.shutdownNow();
			return this;
		}

		@Override public boolean isShutdown() {
			return executor.isShutdown();
		}

		@Override public boolean isTerminated() {
			return executor.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			return executor.awaitTermination(timeout, unit);
		}

		@Override public <InT, OutT> ClientCall<InT, OutT> newCall(
				MethodDescriptor<InT, OutT> methodDescriptor, CallOptions callOptions) {
			throw new UnsupportedOperationException("Not implemented");
		}

		@Override public String authority() {
			throw new UnsupportedOperationException("Not implemented");
		}
	}
}
