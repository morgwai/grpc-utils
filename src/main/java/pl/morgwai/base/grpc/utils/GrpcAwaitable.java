package pl.morgwai.base.grpc.utils;

import io.grpc.*;
import pl.morgwai.base.concurrent.Awaitable;



/**
 * Convenience methods for creating {@link Awaitable}s of gRPC object terminations.
 */
public interface GrpcAwaitable {



	static Awaitable.WithUnit ofTermination(Server server) {
		server.shutdown();
		return server::awaitTermination;
	}



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



	static Awaitable.WithUnit ofTermination(ManagedChannel channel) {
		channel.shutdown();
		return channel::awaitTermination;
	}



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
