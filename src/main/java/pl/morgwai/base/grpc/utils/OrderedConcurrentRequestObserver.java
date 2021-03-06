// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer;
import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer.OutputStream;



/**
 * A {@link ConcurrentRequestObserver} that uses {@link OrderedConcurrentOutputBuffer} to
 * automatically ensure that response messages are sent in order corresponding to request messages
 * order.
 * <p>
 * Note: as only responses to "head" requests are sent directly to clients and rest is buffered,
 * the number of requests processed concurrently should not be set too big to avoid excessive buffer
 * growth.</p>
 */
public class OrderedConcurrentRequestObserver<RequestT, ResponseT>
		extends ConcurrentRequestObserver<RequestT, ResponseT>
		implements StreamObserver<RequestT> {



	/**
	 * See {@link ConcurrentRequestObserver#ConcurrentRequestObserver(ServerCallStreamObserver, int,
	 * BiConsumer, Consumer) super}.
	 */
	public OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int numberOfConcurrentRequests,
		BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		this(responseObserver, numberOfConcurrentRequests);
		this.requestHandler = requestHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * See {@link
	 * ConcurrentRequestObserver#ConcurrentRequestObserver(ServerCallStreamObserver, int) super}.
	 */
	protected OrderedConcurrentRequestObserver(
			ServerCallStreamObserver<ResponseT> responseObserver,
			int numberOfConcurrentRequests) {
		super(responseObserver, numberOfConcurrentRequests);
		buffer = new OrderedConcurrentOutputBuffer<>(new OutputStream<>() {

			@Override public void write(ResponseT message) {
				// other bucket threads may be calling isReady(), onError() etc
				synchronized (lock) {
					responseObserver.onNext(message);
				}
			}

			@Override public void close() {
				// {@link ConcurrentRequestObserver} tracks individual requests and takes care of
				// calling {@code responseObserver.onCompleted()}.
			}
		});
	}

	final OrderedConcurrentOutputBuffer<ResponseT> buffer;



	/**
	 * Constructs a new {@link SingleRequestMessageResponseObserver} that instead of writing
	 * messages directly to the parent response observer, buffers them in its associated bucket.
	 */
	@Override
	protected SingleRequestMessageResponseObserver newSingleRequestMessageResponseObserver() {
		return new BucketResponseObserver(buffer.addBucket());
	}



	class BucketResponseObserver extends SingleRequestMessageResponseObserver {

		final OutputStream<ResponseT> bucket;



		BucketResponseObserver(OutputStream<ResponseT> bucket) {
			this.bucket = bucket;
		}



		@Override
		public void onCompleted() {
			bucket.close();
			super.onCompleted();
		}



		@Override
		public void onNext(ResponseT response) {
			bucket.write(response);
		}
	}
}
