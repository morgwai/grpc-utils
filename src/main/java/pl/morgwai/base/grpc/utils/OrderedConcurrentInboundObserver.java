// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;

import pl.morgwai.base.utils.concurrent.OrderedConcurrentOutputBuffer;
import pl.morgwai.base.utils.concurrent.OrderedConcurrentOutputBuffer.OutputStream;



/**
 * A {@link ConcurrentInboundObserver} that uses {@link OrderedConcurrentOutputBuffer} to ensure
 * that outbound messages are sent in the order corresponding to the inbound messages order.
 * <p>
 * Note: as only results of the processing of a current "head" inbound message are sent directly to
 * the output and the rest is buffered, passing too high
 * {@code maxConcurrentRequestMessages}&nbsp;/&nbsp;{@code maxConcurrentClientResponseMessages}
 * param may lead to "head of the line blocking" resulting in an excessive buffer growth.</p>
 */
public class OrderedConcurrentInboundObserver<InboundT, OutboundT, ControlT>
		extends ConcurrentInboundObserver<InboundT, OutboundT, ControlT> {



	/**
	 * See {@link ConcurrentInboundObserver#newSimpleConcurrentServerRequestObserver(
	 * ServerCallStreamObserver, int, BiConsumer, BiConsumer) "super"}.
	 */
	public static <RequestT, ResponseT>
	OrderedConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
	newSimpleOrderedConcurrentServerRequestObserver(
		ServerCallStreamObserver<ResponseT> responseObserver,
		int maxConcurrentRequestMessages,
		BiConsumer<? super RequestT, ? super SubstreamObserver<ResponseT>> onRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<RequestT, ResponseT, ResponseT>
				> onErrorHandler
	) {
		return new OrderedConcurrentInboundObserver<>(
			responseObserver,
			maxConcurrentRequestMessages,
			onRequestHandler,
			onErrorHandler,
			responseObserver
		);
	}



	/**
	 * See {@link ConcurrentInboundObserver#newConcurrentServerRequestObserver(
	 * ClientCallStreamObserver, int, BiConsumer, BiConsumer, ServerCallStreamObserver) "super"}.
	 */
	public static <ServerRequestT, NestedRequestT, ServerResponseT>
	OrderedConcurrentInboundObserver<ServerRequestT, NestedRequestT, ServerResponseT>
	newOrderedConcurrentServerRequestObserver(
		ClientCallStreamObserver<NestedRequestT> nestedRequestObserver,
		int maxConcurrentServerRequestMessages,
		BiConsumer<? super ServerRequestT, ? super SubstreamObserver<NestedRequestT>>
				onServerRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<
							ServerRequestT, NestedRequestT, ServerResponseT>
				> onErrorHandler,
		ServerCallStreamObserver<ServerResponseT> serverResponseObserver
	) {
		return new OrderedConcurrentInboundObserver<>(
			nestedRequestObserver,
			maxConcurrentServerRequestMessages,
			onServerRequestHandler,
			onErrorHandler,
			serverResponseObserver
		);
	}



	/**
	 * See {@link ConcurrentInboundObserver#newConcurrentClientResponseObserver(CallStreamObserver,
	 * int, BiConsumer, BiConsumer, Consumer) "super"}.
	 */
	public static <ClientResponseT, OutboundT, ClientRequestT>
	OrderedConcurrentInboundObserver<ClientResponseT, OutboundT, ClientRequestT>
	newOrderedConcurrentClientResponseObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentClientResponseMessages,
		BiConsumer<? super ClientResponseT, ? super SubstreamObserver<OutboundT>>
				onClientResponseHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<ClientResponseT, OutboundT, ClientRequestT>
				> onErrorHandler,
		Consumer<? super ClientCallStreamObserver<ClientRequestT>> onBeforeStartHandler
	) {
		return new OrderedConcurrentInboundObserver<>(
			outboundObserver,
			maxConcurrentClientResponseMessages,
			onClientResponseHandler,
			onErrorHandler,
			onBeforeStartHandler
		);
	}



	/**
	 * See {@link ConcurrentInboundObserver#ConcurrentInboundObserver(CallStreamObserver, int,
	 * ServerCallStreamObserver) super}.
	 */
	protected OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentServerRequestMessages,
		ServerCallStreamObserver<ControlT> serverResponseObserver
	) {
		this(
			outboundObserver,
			maxConcurrentServerRequestMessages,
			null,
			null,
			serverResponseObserver
		);
	}



	/**
	 * See {@link ConcurrentInboundObserver#ConcurrentInboundObserver(CallStreamObserver, int)
	 * super}.
	 */
	protected OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentClientResponseMessages
	) {
		this(
			outboundObserver,
			maxConcurrentClientResponseMessages,
			null,
			null,
			(Consumer<ClientCallStreamObserver<ControlT>>) null
		);
	}



	final OrderedConcurrentOutputBuffer<OutboundT> buffer;



	OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentServerRequestMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onServerRequestHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		ServerCallStreamObserver<ControlT> serverResponseObserver
	) {
		super(
			outboundObserver,
			maxConcurrentServerRequestMessages,
			onServerRequestHandler,
			onErrorHandler,
			serverResponseObserver
		);
		buffer = createBuffer(outboundObserver);
	}



	OrderedConcurrentInboundObserver(
		CallStreamObserver<OutboundT> outboundObserver,
		int maxConcurrentClientResponseMessages,
		BiConsumer<? super InboundT, ? super SubstreamObserver<OutboundT>> onClientResponseHandler,
		BiConsumer<
					? super Throwable,
					? super ConcurrentInboundObserver<InboundT, OutboundT, ControlT>
				> onErrorHandler,
		Consumer<? super ClientCallStreamObserver<ControlT>> onBeforeStartHandler
	) {
		super(
			outboundObserver,
			maxConcurrentClientResponseMessages,
			onClientResponseHandler,
			onErrorHandler,
			onBeforeStartHandler
		);
		buffer = createBuffer(outboundObserver);
	}



	private OrderedConcurrentOutputBuffer<OutboundT> createBuffer(
		CallStreamObserver<? super OutboundT> outboundObserver
	) {
		return new OrderedConcurrentOutputBuffer<>(new OutputStream<>() {

			@Override public void write(OutboundT message) {
				// threads from other buckets may be calling isReady(), onError() etc
				synchronized (lock) {
					outboundObserver.onNext(message);
				}
			}

			@Override public void close() {
				 // ConcurrentInboundObserver tracks substream observers and calls
				 // outboundObserver.onCompleted() when all substreams are completed.
			}
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
