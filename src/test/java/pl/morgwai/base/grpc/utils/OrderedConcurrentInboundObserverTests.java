// Copyright 2022 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
package pl.morgwai.base.grpc.utils;

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Comparators;
import org.junit.experimental.categories.Category;



public abstract class OrderedConcurrentInboundObserverTests extends ConcurrentInboundObserverTests {



	@Override
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms2kMsgs()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferOftenUnreadyFor3ms2kMsgs();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	@Category(SlowTests.class)
	public void testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads3kMsgs()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferOftenUnreadyFor3msTooFewThreads3kMsgs();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays200kMsgs()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferVeryOftenUnreadyFor0msNoDelays200kMsgs();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms2kMsgs()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferSometimesUnreadyFor15ms2kMsgs();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads2kMsgs()
			throws InterruptedException {
		super.testAsyncConcurrentProcessingBufferSometimesUnreadyFor15msTooFewThreads2kMsgs();
		assertTrue("messages should be written in order", Comparators.isInStrictOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}



	@Override public void
	testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms500msgs()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferOftenUnreadyFor3ms500msgs();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override public
	void testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays10kMsgs()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelays10kMsgs();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override public
	void testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads()
			throws InterruptedException {
		super.
		testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override public void
	testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads10kMsgs
			() throws InterruptedException {
		super.
	testOnReadyHandlerIntegrationMultiThreadBufferVeryOftenUnreadyFor0msNoDelaysTooFewThreads10kMsgs
			();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}

	@Override
	public void testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms400msgs()
			throws InterruptedException {
		super.testOnReadyHandlerIntegrationMultiThreadBufferSometimesUnreadyFor15ms400msgs();
		assertTrue("messages should be written in order", Comparators.isInOrder(
				fakeOutboundObserver.getOutputData(), outboundMessageComparator));
	}


}
