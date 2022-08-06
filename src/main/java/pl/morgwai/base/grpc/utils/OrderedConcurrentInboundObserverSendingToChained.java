// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



/**
 * TODO
 * @param <InboundT>
 * @param <ChainedRequestT>
 * @param <ControlT>
 */
public class OrderedConcurrentInboundObserverSendingToChained<
				InboundT, ChainedRequestT, ControlT>
		extends OrderedConcurrentInboundObserver<InboundT, ChainedRequestT, ControlT> {



	/**
	 * Produces response messages to the given {@code inboundMessage}. Responses must be submitted
	 * to {@code individualInboundMessageResultObserver} that is associated with this
	 * {@code inboundMessage} using {@link CallStreamObserver#onNext(Object)
	 * individualInboundMessageResultObserver.onNext(response)}.
	 * Once all responses to this {@code inboundMessage} are submitted, this method must call
	 * {@link IndividualInboundMessageResultObserver#onCompleted()
	 * individualInboundMessageResultObserver.onComplete()}.
	 * <p>
	 * {@code individualInboundMessageResultObserver} is thread-safe and implementations of this
	 * method may freely dispatch work to several other threads.</p>
	 * <p>
	 * {@code individualInboundMessageResultObserver.cancel(...)} can only be called if the parent
	 * outbound observer is a {@link ClientCallStreamObserver}.</p>
	 * <p>
	 * To avoid excessive buffering, implementations should respect
	 * {@code individualInboundMessageResultObserver}'s readiness with
	 * {@link CallStreamObserver#isReady() individualInboundMessageResultObserver.isReady()} and
	 * {@link CallStreamObserver#setOnReadyHandler(Runnable)
	 * individualInboundMessageResultObserver.setOnReadyHandler(...)} methods.<br/>
	 * Consider using {@link DispatchingOnReadyHandler} or
	 * {@link StreamObservers#copyWithFlowControl(Iterable, CallStreamObserver)}.</p>
	 * <p>
	 * Default implementation calls {@link #inboundMessageHandler}.</p>
	 *
	 * @see IndividualInboundMessageResultObserver
	 */
	protected void onInboundMessage(
		InboundT inboundMessage,
		ClientCallStreamObserver<ChainedRequestT> individualInboundMessageResultObserver
	) {
		inboundMessageHandler.accept(inboundMessage, individualInboundMessageResultObserver);
	}

	/**
	 * Called by {@link #onInboundMessage(Object, ClientCallStreamObserver)}.
	 * Initialized via the param of
	 * {@link #OrderedConcurrentInboundObserverSendingToChained(ClientCallStreamObserver, int,
	 * BiConsumer, Consumer, ServerCallStreamObserver)} and
	 * {@link #OrderedConcurrentInboundObserverSendingToChained(ClientCallStreamObserver, int,
	 * BiConsumer, Consumer, Consumer)} constructors.
	 */
	protected BiConsumer<InboundT, ClientCallStreamObserver<ChainedRequestT>> inboundMessageHandler;



	protected OrderedConcurrentInboundObserverSendingToChained(
		ClientCallStreamObserver<ChainedRequestT> clientRequestObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, ClientCallStreamObserver<ChainedRequestT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		super(clientRequestObserver, numberOfInitialMessages, errorHandler, null);
		this.inboundMessageHandler = inboundMessageHandler;
		setInboundControlObserver(inboundControlObserver);
	}

	protected OrderedConcurrentInboundObserverSendingToChained(
		ClientCallStreamObserver<ChainedRequestT> clientRequestObserver,
		int numberOfInitialMessages,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		super(clientRequestObserver, numberOfInitialMessages, null, null);
		setInboundControlObserver(inboundControlObserver);
	}



	protected OrderedConcurrentInboundObserverSendingToChained(
		ClientCallStreamObserver<ChainedRequestT> clientRequestObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, ClientCallStreamObserver<ChainedRequestT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		super(clientRequestObserver, numberOfInitialMessages, errorHandler, preStartHandler);
		this.inboundMessageHandler = inboundMessageHandler;
	}

	protected OrderedConcurrentInboundObserverSendingToChained(
		ClientCallStreamObserver<ChainedRequestT> clientRequestObserver,
		int numberOfInitialMessages
	) {
		super(clientRequestObserver, numberOfInitialMessages, null, null);
	}



	/**
	 * Calls {@link #onInboundMessage(Object, ClientCallStreamObserver)
	 * onRequest}({@code request}, {@link #newIndividualObserver()}) and if the outbound observer is
	 * ready, then also {@code individualObserver.onReadyHandler.run()}.
	 */
	@Override
	public final void onNext(InboundT request) {
		final var individualObserver = newIndividualObserver();
		onInboundMessage(request, individualObserver);
		synchronized (lock) {
			if ( !outboundObserver.isReady()) return;
		}
		if (individualObserver.onReadyHandler != null) individualObserver.onReadyHandler.run();
	}
}
