// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;

import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer;
import pl.morgwai.base.concurrent.OrderedConcurrentOutputBuffer.OutputStream;



/**
 * A {@link ConcurrentInboundObserver} that uses {@link OrderedConcurrentOutputBuffer} to ensure
 * that outbound messages are sent in the order corresponding to the inbound messages order.
 * <p>
 * Note: as only results of the processing of a current "head" inbound message are sent directly to
 * the output and the rest is buffered, passing too high {@code maxConcurrentMessages} constructor
 * param may lead to "head of the line blocking" resulting in an excessive buffer growth.</p>
 */
public class OrderedConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		extends ConcurrentInboundObserver<InboundT, OutboundT, ControlT> {



	final OrderedConcurrentOutputBuffer<OutboundT> buffer;



	/**
	 * Use {@link #newOrderedConcurrentServerRequestObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, ServerCallStreamObserver)} instead.
	 */
	@Deprecated(forRemoval = true)
	public OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> inboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		super(outboundObserver, maxConcurrentMessages, inboundMessageHandler, onErrorHandler,
				inboundControlObserver);
		buffer = createBuffer(outboundObserver);
	}

	/**
	 * See {@link ConcurrentInboundObserver#newConcurrentServerRequestObserver(CallStreamObserver,
	 * int, BiConsumer, BiConsumer, ServerCallStreamObserver) "super"}.
	 */
	public static <InboundT, OutboundT, ControlT>
	OrderedConcurrentInboundObserver<InboundT, OutboundT, ControlT>
	newOrderedConcurrentServerRequestObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		return new OrderedConcurrentInboundObserver<>(
			outboundObserver,
			maxConcurrentMessages,
			onInboundMessageHandler,
			onErrorHandler,
			inboundControlObserver
		);
	}

	/**
	 * See {@link ConcurrentInboundObserver#newSimpleConcurrentServerRequestObserver(
	 * ServerCallStreamObserver, int, BiConsumer, BiConsumer) "super"}.
	 */
	public static <InboundT, OutboundT>
	OrderedConcurrentInboundObserver<InboundT, OutboundT, OutboundT>
	newSimpleOrderedConcurrentServerRequestObserver(
		ServerCallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<
							? super InboundT, ? super OutboundT, ? super OutboundT>
				> onErrorHandler
	) {
		return newOrderedConcurrentServerRequestObserver(
			outboundObserver,
			maxConcurrentMessages,
			onInboundMessageHandler,
			onErrorHandler,
			outboundObserver
		);
	}

	/**
	 * See {@link ConcurrentInboundObserver#ConcurrentInboundObserver(CallStreamObserver, int,
	 * ServerCallStreamObserver) super}.
	 */
	protected OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		super(outboundObserver, maxConcurrentMessages, null, null, inboundControlObserver);
		buffer = createBuffer(outboundObserver);
	}

	/**
	 * Use {@link #newOrderedConcurrentClientResponseObserver(CallStreamObserver, int, BiConsumer,
	 * BiConsumer, Consumer)} instead.
	 */
	@Deprecated(forRemoval = true)
	public OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> inboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		Consumer<ClientCallStreamObserver<? super ControlT>> preStartHandler
	) {
		super(outboundObserver, maxConcurrentMessages, inboundMessageHandler, onErrorHandler,
				preStartHandler);
		buffer = createBuffer(outboundObserver);
	}

	/**
	 * See {@link ConcurrentInboundObserver#newConcurrentClientResponseObserver(CallStreamObserver,
	 * int, BiConsumer, BiConsumer, Consumer) "super"}.
	 */
	public static <InboundT, OutboundT, ControlT>
	OrderedConcurrentInboundObserver<InboundT, OutboundT, ControlT>
	newOrderedConcurrentClientResponseObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages,
		BiConsumer<? super InboundT, CallStreamObserver<? super OutboundT>> onInboundMessageHandler,
		BiConsumer<
					? super Throwable,
					ConcurrentInboundObserver<? super InboundT, ? super OutboundT, ? super ControlT>
				> onErrorHandler,
		Consumer<ClientCallStreamObserver<? super ControlT>> onBeforeStartHandler
	) {
		return new OrderedConcurrentInboundObserver<>(
			outboundObserver,
			maxConcurrentMessages,
			onInboundMessageHandler,
			onErrorHandler,
			onBeforeStartHandler
		);
	}

	/**
	 * See {@link ConcurrentInboundObserver#ConcurrentInboundObserver(CallStreamObserver, int)
	 * super}.
	 */
	protected OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentMessages
	) {
		super(outboundObserver, maxConcurrentMessages, null, null,
			(Consumer<ClientCallStreamObserver<? super ControlT>>) null);
		buffer = createBuffer(outboundObserver);
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
			 * {@link ConcurrentInboundObserver} tracks individual messages and takes care of
			 * calling {@code outboundObserver.onCompleted()}.
			 */
			@Override public void close() {}
		});
	}



	/**
	 * Constructs a new {@link OutboundBucketObserver substream observer} that instead of
	 * writing messages directly to the parent {@code outboundObserver}, buffers them in its
	 * associated bucket.
	 * <p>
	 * <b>NOTE:</b> Applications that create additional outbound substreams, should be very wary as
	 * all buckets associated with subsequently received inbound messages will be buffered until the
	 * additionally created substream is completed.</p>
	 */
	@Override
	public OutboundSubstreamObserver newOutboundSubstream() {
		return new OutboundBucketObserver(buffer.addBucket());
	}



	/**
	 * An {@link OutboundBucketObserver OutboundSubstreamObserver} that instead of writing messages
	 * directly to the parent outbound observer, buffers them in its associated bucket.
	 */
	public class OutboundBucketObserver extends OutboundSubstreamObserver {

		final OutputStream<OutboundT> bucket;



		protected OutboundBucketObserver(OutputStream<OutboundT> bucket) {
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
