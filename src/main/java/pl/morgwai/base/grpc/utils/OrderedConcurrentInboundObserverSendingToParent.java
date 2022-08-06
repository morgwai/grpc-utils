// Copyright (c) Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.grpc.stub.*;



/**
 * TODO
 * @param <InboundT>
 * @param <ParentResponseT>
 * @param <ControlT>
 */
public class OrderedConcurrentInboundObserverSendingToParent<InboundT, ParentResponseT, ControlT>
		extends OrderedConcurrentInboundObserver<InboundT, ParentResponseT, ControlT> {



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
		CallStreamObserver<ParentResponseT> individualInboundMessageResultObserver
	) {
		inboundMessageHandler.accept(inboundMessage, individualInboundMessageResultObserver);
	}

	/**
	 * Called by {@link #onInboundMessage(Object, CallStreamObserver)}.
	 * Initialized via the param of
	 * {@link #OrderedConcurrentInboundObserverSendingToParent(ServerCallStreamObserver, int,
	 * BiConsumer, Consumer)} and
	 * {@link #OrderedConcurrentInboundObserverSendingToParent(ServerCallStreamObserver, int,
	 * BiConsumer, Consumer, Consumer)} constructors.
	 */
	protected BiConsumer<InboundT, CallStreamObserver<ParentResponseT>> inboundMessageHandler;



	protected OrderedConcurrentInboundObserverSendingToParent(
		ServerCallStreamObserver<ParentResponseT> serverResponseObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, CallStreamObserver<ParentResponseT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler
	) {
		super(serverResponseObserver, numberOfInitialMessages, errorHandler, null);
		this.inboundMessageHandler = inboundMessageHandler;
		setInboundControlObserver(serverResponseObserver);
	}

	protected OrderedConcurrentInboundObserverSendingToParent(
		ServerCallStreamObserver<ParentResponseT> serverResponseObserver,
		int numberOfInitialMessages,
		ServerCallStreamObserver<ControlT> inboundControlObserver
	) {
		super(serverResponseObserver, numberOfInitialMessages, null, null);
		setInboundControlObserver(inboundControlObserver);
	}



	protected OrderedConcurrentInboundObserverSendingToParent(
		ServerCallStreamObserver<ParentResponseT> serverResponseObserver,
		int numberOfInitialMessages,
		BiConsumer<InboundT, CallStreamObserver<ParentResponseT>> inboundMessageHandler,
		Consumer<Throwable> errorHandler,
		Consumer<ClientCallStreamObserver<ControlT>> preStartHandler
	) {
		super(serverResponseObserver, numberOfInitialMessages, errorHandler, preStartHandler);
		this.inboundMessageHandler = inboundMessageHandler;
	}

	protected OrderedConcurrentInboundObserverSendingToParent(
		ServerCallStreamObserver<ParentResponseT> serverResponseObserver,
		int numberOfInitialMessages
	) {
		super(serverResponseObserver, numberOfInitialMessages, null, null);
	}



	/**
	 * Calls {@link #onInboundMessage(Object, CallStreamObserver)
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
