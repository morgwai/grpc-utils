// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.Consumer;

import io.grpc.stub.*;

import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer;
import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer.OutputStream;



/**
 * A {@link ConcurrentInboundObserver} that uses {@link OrderedConcurrentOutputBuffer} to
 * automatically ensure that outbound messages are sent in the order corresponding to inbound
 * messages order.
 * <p>
 * Note: as only results of the processing of a current "head" inbound message are sent directly to
 * the output and the rest is buffered, setting the number of concurrently processed inbound
 * messages too high may lead to "head of the line blocking" resulting in an excessive buffer
 * growth.</p>
 */
public abstract class OrderedConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		extends ConcurrentInboundObserver<InboundT, OutboundT, ControlT> {



	final OrderedConcurrentOutputBuffer<OutboundT> buffer;



	protected OrderedConcurrentInboundObserver(
		ServerCallStreamObserver<OutboundT> serverResponseObserver,
		int numberOfInitialMessages,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		super(serverResponseObserver, numberOfInitialMessages, errorHandler, preStartHandler);
		buffer = createBuffer(serverResponseObserver);
	}

	protected OrderedConcurrentInboundObserver(
		ClientCallStreamObserver<OutboundT> clientRequestObserver,
		int numberOfInitialMessages,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		super(clientRequestObserver, numberOfInitialMessages, errorHandler, preStartHandler);
		buffer = createBuffer(clientRequestObserver);
	}

	private OrderedConcurrentOutputBuffer<OutboundT> createBuffer(
			CallStreamObserver<OutboundT> responseObserver) {
		return new OrderedConcurrentOutputBuffer<>(new OutputStream<>() {

			@Override public void write(OutboundT message) {
				// other bucket threads may be calling isReady(), onError() etc
				synchronized (lock) {
					responseObserver.onNext(message);
				}
			}

			/**
			 * {@link ConcurrentInboundObserver} tracks individual requests and takes care of
			 * calling {@code responseObserver.onCompleted()}.
			 */
			@Override public void close() {}
		});
	}



	/**
	 * Constructs a new {@link OutboundBucketObserver IndividualInboundMessageResultObserver} that
	 * instead of writing messages directly to the parent outbound observer, buffers them in its
	 * associated bucket.
	 * <b>NOTE:</b> Applications that create additional individual observers, should be very wary as
	 * all buckets associated with subsequently received inbound messages will be buffered until the
	 * additionally created observer is closed.
	 */
	@Override
	public IndividualInboundMessageResultObserver newIndividualObserver() {
		return new OutboundBucketObserver(buffer.addBucket());
	}



	/**
	 * An {@link IndividualInboundMessageResultObserver} that instead of writing messages directly
	 * to the parent outbound observer, buffers them in its associated bucket.
	 */
	protected class OutboundBucketObserver extends IndividualInboundMessageResultObserver {

		final OutputStream<OutboundT> bucket;



		OutboundBucketObserver(OutputStream<OutboundT> bucket) {
			this.bucket = bucket;
		}



		@Override
		public void onCompleted() {
			bucket.close();
			super.onCompleted();
		}



		@Override
		public void onNext(OutboundT message) {
			bucket.write(message);
		}
	}
}
