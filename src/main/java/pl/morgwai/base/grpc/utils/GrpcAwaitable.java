// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.concurrent.TimeUnit;

import io.grpc.*;
import pl.morgwai.base.concurrent.Awaitable;



/**
 * Convenience functions for creating {@link Awaitable}s of gRPC objects terminations.
 */
public interface GrpcAwaitable {



	/**
	 * Creates {@link Awaitable.WithUnit} of
	 * {@link Server#awaitTermination(long, TimeUnit) termination} of {@code server}.
	 */
	static Awaitable.WithUnit ofTermination(Server server) {
		return (timeout, unit) -> {
			server.shutdown();
			return server.awaitTermination(timeout, unit);
		};
	}



	/**
	 * Creates {@link Awaitable.WithUnit} of
	 * {@link Server#awaitTermination(long, TimeUnit) termination} of {@code server}.
	 * If {@code server} fails to terminate, {@link Server#shutdownNow()} is called.
	 */
	static Awaitable.WithUnit ofEnforcedTermination(Server server) {
		return (timeout, unit) -> {
			try {
				server.shutdown();
				return server.awaitTermination(timeout, unit);
			} finally {
				if ( !server.isTerminated()) server.shutdownNow();
			}
		};
	}



	/**
	 * Creates {@link Awaitable.WithUnit} of
	 * {@link ManagedChannel#awaitTermination(long, TimeUnit) termination} of {@code channel}.
	 */
	static Awaitable.WithUnit ofTermination(ManagedChannel channel) {
		return (timeout, unit) -> {
			channel.shutdown();
			return channel.awaitTermination(timeout, unit);
		};
	}



	/**
	 * Creates {@link Awaitable.WithUnit} of
	 * {@link ManagedChannel#awaitTermination(long, TimeUnit) termination} of {@code channel}.
	 * If {@code channel} fails to terminate, {@link ManagedChannel#shutdownNow()} is called.
	 */
	static Awaitable.WithUnit ofEnforcedTermination(ManagedChannel channel) {
		return (timeout, unit) -> {
			try {
				channel.shutdown();
				return channel.awaitTermination(timeout, unit);
			} finally {
				if ( !channel.isTerminated()) channel.shutdownNow();
			}
		};
	}
}
