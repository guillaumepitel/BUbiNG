package it.unimi.di.law.bubing.frontier.comm;

/*
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.exensa.wdl.common.Serializer;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.Frontier;
import it.unimi.di.law.bubing.util.*;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//RELEASE-STATUS: DIST

/** A set of Pulsar Receivers
 */
public final class CrawlRequestsReceiver implements MessageListener<byte[]>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(CrawlRequestsReceiver.class);

	/** A reference to the frontier. */
	private final Frontier frontier;

	private long messageCount;

	/** Creates the thread.
	 *
	 * @param frontier the frontier instantiating this thread.
	 */
	public CrawlRequestsReceiver(final Frontier frontier) {
		this.frontier = frontier;
		this.messageCount = 0;
	}

	@Override
	public void received( final Consumer<byte[]> consumer, final Message<byte[]> message ) {
		try {
			final MsgFrontier.CrawlRequest crawlRequest = MsgFrontier.CrawlRequest.parseFrom( message.getData() );
			if ( LOGGER.isTraceEnabled() ) LOGGER.trace( "Received url {} to crawl", Serializer.URL.Key.toString(crawlRequest.getUrlKey()) );
			frontier.quickReceivedCrawlRequests.put( crawlRequest ); // Will block until not full
			frontier.numberOfReceivedURLs.addAndGet( 1 );
			messageCount++;
			if (messageCount == 1000)
				LOGGER.warn("PULSAR Consumer is active");
			consumer.acknowledge( message );
		}
		catch ( InvalidProtocolBufferException e ) {
			LOGGER.error("Error while parsing message", e);
		}
		catch ( InterruptedException e ) {
			LOGGER.error("Interrupted while processing message", e);
		}
		catch ( PulsarClientException e ) {
		  LOGGER.error("While acknowledging message", e);
    }
	}
}
