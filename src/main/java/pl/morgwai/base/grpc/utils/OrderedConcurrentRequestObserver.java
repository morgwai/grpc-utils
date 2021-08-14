// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import pl.morgwai.base.utils.OrderedConcurrentOutputBuffer;
import pl.morgwai.base.utils.OrderedConcurrentOutputBuffer.OutputStream;



/**
 * A {@link ConcurrentRequestObserver} that uses {@link OrderedConcurrentOutputBuffer} to
 * automatically ensure that response messages are sent in order corresponding to request messages
 * order.<br/>
 * Note: as only responses to "head" request are sent directly to a client and rest is buffered,
 * the concurrency level (number of requests processed concurrently, determined by the initial
 * {@link CallStreamObserver#request(int)} call) should not be too big to avoid excessive buffer
 * growth.
 */
public class OrderedConcurrentRequestObserver<RequestT, ResponseT>
		extends ConcurrentRequestObserver<RequestT, ResponseT>
		implements StreamObserver<RequestT> {



	public OrderedConcurrentRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		BiConsumer<RequestT, CallStreamObserver<ResponseT>> requestHandler,
		Consumer<Throwable> errorHandler
	) {
		this(responseObserver);
		this.requestHandler = requestHandler;
		this.errorHandler = errorHandler;
	}



	/**
	 * Constructor for those who prefer to override {@link #onRequest(Object, CallStreamObserver)}
	 * and {@link #onError(Throwable)} in a subclass instead of providing lambdas.
	 */
	protected OrderedConcurrentRequestObserver(
			ServerCallStreamObserver<ResponseT> responseObserver) {
		super(responseObserver);
		buffer = new OrderedConcurrentOutputBuffer<>(new OutputStream<>() {

			@Override
			public void write(ResponseT message) {
				synchronized (OrderedConcurrentRequestObserver.this) {
					responseObserver.onNext(message);
				}
			}

			/**
			 * {@link ConcurrentRequestObserver} tracks individual requests and takes care of
			 * calling {@code onCompleted()}.
			 */
			@Override
			public void close() {}
		});
	}

	final OrderedConcurrentOutputBuffer<ResponseT> buffer;
	volatile boolean error = false;



	@Override
	protected SingleRequestMessageResponseObserver newSingleRequestMessageResponseObserver() {
		return new BucketResponseObserver(buffer.addBucket());
	}



	class BucketResponseObserver extends SingleRequestMessageResponseObserver {

		final OrderedConcurrentOutputBuffer<ResponseT>.Bucket bucket;



		BucketResponseObserver(OrderedConcurrentOutputBuffer<ResponseT>.Bucket bucket) {
			this.bucket = bucket;
		}



		@Override
		public void onCompleted() {
			if (error) {
				throw new IllegalStateException(ERROR_REPORTED_MESSAGE);
			}
			bucket.close();
			super.onCompleted();
		}



		@Override
		public void onNext(ResponseT response) {
			if (error) {
				throw new IllegalStateException(ERROR_REPORTED_MESSAGE);
			}
			bucket.write(response);
		}



		@Override
		public void onError(Throwable t) {
			error = true;
			super.onError(t);
		}
	}



	static final String ERROR_REPORTED_MESSAGE = "onError() has been called";
}
