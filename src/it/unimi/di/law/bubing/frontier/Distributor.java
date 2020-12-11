package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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


import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.protobuf.ProtoHelper;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.dsi.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

//RELEASE-STATUS: DIST

/** A thread that distributes {@linkplain Frontier#receivedCrawlRequests ready URLs} into
 *  the {@link Workbench} queues with the help of a {@link WorkbenchVirtualizer}.
 *  We invite the reader to consult the documentation of {@link WorkbenchVirtualizer} first.
 *
 *  <p>The behaviour of this distributor is controlled by two conditions:
 *  <ol>
 *  	<li>Whether the workbench is full.
 *  	<li>Whether the front is large enough, that is, whether the number of visit states
 *  		in the {@linkplain Frontier#todo todo list} (which are actually thought as being representative of their own IP address)
 *          plus {@link Frontier#workbench}.size()
 *  		is below the current required size (i.e., the front is counted in IP addresses).
 *  </ol>
 *
 *  <p>Then, this distributor iteratively:
 *  <ul>
 *  	<li>Checks that the workbench is not full and that the front is not large enough. If either condition
 *  	fails, there is no point in doing I/O.
 *  	<li>If the conditions are fulfilled, the distributor checks the {@link Frontier#refill} queue
 *		to see whether there are visit states requiring a refill from the {@link WorkbenchVirtualizer},
 *		in which case it performs a refill.
 *  	<li>Otherwise, if there are no ready URLs and it is too early to force a flush of the sieve, this thread is put
 *      to sleep with an exponential backoff.
 *      <li>Otherwise, (possibly after a flush) a ready URL is loaded from {@link Frontier#receivedCrawlRequests} and either deleted
 *      (if we already have too many URLs for its scheme+authority),
 *      or enqueued to the workbench (if its visit state has no virtualized URLs and has not reached {@link VisitState#pathQueryLimit()}),
 *      or otherwise enqueued to the {@link WorkbenchVirtualizer}.
 *  </ul>
 *
 */
public final class Distributor extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(Distributor.class);
	/** We purge {@linkplain VisitState visit states} from {@link #schemeAuthority2VisitState} when
	 * this amount of time has passed (approximately) since the last fetch. */
	private static final long PURGE_DELAY = TimeUnit.MINUTES.toMillis(149);
	/** We prints low-cost stats at this interval. */
	private static final long LOW_COST_STATS_INTERVAL = TimeUnit.MINUTES.toMillis(11);
	/** We prints high-cost stats at this interval. */
	private static final long HIGH_COST_STATS_INTERVAL = TimeUnit.MINUTES.toMillis(47);
	/** We check for visit states to be purged at this interval. */
	private static final long PURGE_CHECK_INTERVAL = TimeUnit.MINUTES.toMillis(97);

	/** A reference to the frontier. */
	private final Frontier frontier;
	/** An <strong>unsynchronized</strong> map from scheme+authorities to the corresponding {@link VisitState}. */
	protected final VisitStateSet schemeAuthority2VisitState;
	/** The thread printing statistics. */
	protected final StatsThread statsThread;
  /** The last time we produced a low-cost statistics. */
  private long lastLowCostStats;
	/** The last time we produced a high-cost statistics. */
	volatile long lastHighCostStats;
	/** The last time we checked for visit states to be purged. */
	private long lastPurgeCheck;

	private long movedFromReceivedToVirtualizer = 0, movedFromReceivedToOverflow = 0, movedFromReceivedToWorkbench = 0, deletedFromReceived = 0;

	/** stats */
	private long movedFromQueues = 0;
	private long deletedFromQueues = 0;
	/** low cost stats */
	private long fullWorkbenchSleepTime = 0;
	private long largeFrontSleepTime = 0;
	private long noReadyURLsSleepTime = 0;

  /** Creates a distributor for the given frontier.
	 *
	 * @param frontier the frontier instantiating this distribution.
	 */
	public Distributor(final Frontier frontier) {
		this.frontier = frontier;
		this.schemeAuthority2VisitState = new VisitStateSet();
		setName(this.getClass().getSimpleName());
		setPriority(Thread.MAX_PRIORITY);
		statsThread = new StatsThread(frontier, this);
	}

	private boolean processURL(final MsgFrontier.CrawlRequest crawlRequest, long now) {
		try {
			if (LOGGER.isTraceEnabled())
				LOGGER.trace("Processing URL : {}", Serializer.URL.Key.toString(crawlRequest.getUrlKey()));
			if (ProtoHelper.ttlHasExpired(crawlRequest.getCrawlInfo().getScheduleTimeMinutes(), frontier.rc.crawlRequestTTL)) {
				if (LOGGER.isTraceEnabled())
					LOGGER.trace("CrawlRequest has expired for {}", Serializer.URL.Key.toString(crawlRequest.getUrlKey()));
				return false;
			}
			VisitState visitState;
			byte[] schemeAuthority = PulsarHelper.schemeAuthority(crawlRequest.getUrlKey());
			boolean addedNewVisitState = false;
			visitState = schemeAuthority2VisitState.get(schemeAuthority, 0, schemeAuthority.length);
			if (visitState == null) {
					if (LOGGER.isTraceEnabled())
						LOGGER.trace("New scheme+authority {} with path+query {}",
								it.unimi.di.law.bubing.util.Util.toString(schemeAuthority),
								new String(HuffmanModel.defaultModel.decompress(crawlRequest.getUrlKey().getZPathQuery().toByteArray())));
					visitState = new VisitState(schemeAuthority);
					visitState.lastRobotsFetch = Long.MAX_VALUE; // This inhibits further enqueueing until robots.txt is fetched.
					visitState.enqueueRobots();
					visitState.enqueueCrawlRequest(PulsarHelper.toMinimalCrawlRequestSerialized(crawlRequest));
					synchronized (schemeAuthority2VisitState) {
						schemeAuthority2VisitState.add(visitState);
					}
					// Send the visit state to the DNS threads
					frontier.newVisitStates.add(visitState);
					frontier.receivedVisitStates.incrementAndGet();
					movedFromReceivedToWorkbench++;
					addedNewVisitState = true;
			}
			else {
				if (frontier.virtualizer.count(visitState) > 0) {
					// Safe: there are URLs on disk, and this fact cannot change concurrently.
					movedFromReceivedToVirtualizer++;
					frontier.virtualizer.enqueueCrawlRequest(visitState, PulsarHelper.toMinimalCrawlRequestSerialized(crawlRequest));
				}
				else
				if (visitState.size() < visitState.pathQueryLimit() && visitState.workbenchEntry != null && visitState.lastExceptionClass == null) {
					/* Safe: we are enqueueing to a sane (modulo race conditions)
					 * visit state, which will be necessarily go through the DoneThread later. */
					if (visitState.size() == 0)
						addedNewVisitState = true;
					visitState.checkRobots(now);
					visitState.enqueueCrawlRequest(PulsarHelper.toMinimalCrawlRequestSerialized(crawlRequest));
					movedFromReceivedToWorkbench++;
				}
				else { // visitState.urlsOnDisk == 0
					// FIXME: we are here not only because visitState.urlsOnDisk == 0
					movedFromReceivedToVirtualizer++;
					frontier.virtualizer.enqueueCrawlRequest(visitState, PulsarHelper.toMinimalCrawlRequestSerialized(crawlRequest));
				}
			}
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception", t);
			return false;
		}
		return true;
	}

	@Override
	public void run() {
		try {
			/* During the following loop, you should set round to -1 every time something useful is done (e.g., a URL is read from the sieve, or from the virtual queues etc.) */
			for ( int round=0; ; round += 1 ) {
				frontier.rc.ensureNotPaused();
				if (frontier.rc.stopping) break;
				long now = System.currentTimeMillis();

				final boolean workbenchIsFull = frontier.workbenchIsFull();
				final boolean frontIsSmall = frontIsSmall();

				/* The basic logic of workbench updates is that if the front is large enough, we don't do anything.
				 * In this way we both automatically batch disk reads and reduce core memory usage. The required
				 * front size is adaptively set by FetchingThread instances when they detect that the
				 * visit states in the todo list plus workbench.size() is below the current required size
				 * (i.e., we are counting IPs). */
				if ( !workbenchIsFull ) {
					VisitState visitState = frontier.refill.poll();
					if (visitState != null) { // The priority is given to already started visits
						round = -1;
						if ( frontier.virtualizer.count(visitState) == 0 )
						  LOGGER.info( "No URLs on disk during refill: " + visitState );
						else {
							// Note that this might make temporarily the workbench too big by a little bit.
							final int pathQueryLimit = visitState.pathQueryLimit();
              if (LOGGER.isDebugEnabled()) LOGGER.debug("Refilling {} with {} URLs", visitState, pathQueryLimit);
							visitState.checkRobots(now);
							final int dequeuedURLs = frontier.virtualizer.dequeueCrawlRequests(visitState, pathQueryLimit);
							movedFromQueues += dequeuedURLs;
						}
					}

					if ( frontIsSmall ) {
						// It is necessary to enrich the workbench picking up URLs from the sieve
            int toAdd = Math.min( 1000, frontier.receivedCrawlRequests.size() );
						// Note that this might make temporarily the workbench too big by a little bit.
            while ( toAdd > 0 && !frontier.receivedCrawlRequests.isEmpty() ) {
              round = -1;
              final MsgFrontier.CrawlRequest crawlRequest = frontier.receivedCrawlRequests.take();
              if ( processURL(crawlRequest,now) )
                toAdd -= 1;
            }
					}
				}

				if ( now - LOW_COST_STATS_INTERVAL > lastLowCostStats ) {
				  doLowCostStats();
				  lastLowCostStats = now;
				}

				if ( now - HIGH_COST_STATS_INTERVAL > lastHighCostStats ) {
					lastHighCostStats = Long.MAX_VALUE;
					startHighCostStats();
				}

				if ( now - PURGE_CHECK_INTERVAL > lastPurgeCheck ) {
				  doPurgeCheck( now );
					lastPurgeCheck = now;
				}

				if ( round != -1 ) {
					final int sleepTime = 1 << Math.min(10, round);
					if (workbenchIsFull) fullWorkbenchSleepTime += sleepTime;
					else if (!frontIsSmall) largeFrontSleepTime += sleepTime;
					else noReadyURLsSleepTime += sleepTime;
					if (frontier.rc.stopping) break;
					Thread.sleep(sleepTime);
				}
			}

			LOGGER.info("Completed.");
		}
		catch (Throwable t) {
			LOGGER.error("Unexpected exception", t);
		}
	}

	/** Determines whether the front is small. The front size (in IPs) is obtained by adding the size of the {@link Frontier#todo} list and
	 *  the number of nonbroken workbench entries (i.e., IPs that are in the workbench having at least one nonbroken {@link VisitState} in them).
	 *  The front size is considered to be small if it is smaller than {@link Frontier#requiredFrontSize}.
	 *
	 * @return whether the front is small.
	 */
	private boolean frontIsSmall() {
		return frontier.getCurrentFrontSize() <= frontier.requiredFrontSize.get();
	}

	private void doLowCostStats() throws IOException {
		final long overallReceived = deletedFromReceived + movedFromReceivedToWorkbench + movedFromReceivedToVirtualizer + movedFromReceivedToOverflow;
		if ( overallReceived != 0 )
		  LOGGER.info(String.format(
		    "Moved %,d URLs from received (%s%% deleted, %s%% to workbench, %s%% to virtual queues, %s%% to overflow)",
        overallReceived,
        Util.format(100.0 * deletedFromReceived / overallReceived),
        Util.format(100.0 * movedFromReceivedToWorkbench / overallReceived),
        Util.format(100.0 * movedFromReceivedToVirtualizer / overallReceived),
        Util.format(100.0 * movedFromReceivedToOverflow / overallReceived)
      ));
    deletedFromReceived = movedFromReceivedToWorkbench = movedFromReceivedToVirtualizer = movedFromReceivedToOverflow = 0;

    final long overallQueues = movedFromQueues + deletedFromQueues;
		if ( overallQueues != 0 )
		  LOGGER.info(String.format(
		    "Moved %,d URLs from queues (%s%% deleted)",
        overallQueues,
        Util.format(100.0 * deletedFromQueues / overallQueues)
      ));
    movedFromQueues = deletedFromQueues = 0;

		LOGGER.info(String.format(
		  "Sleeping: large front %,d, full workbench %,d, no ready URLs %,d",
      largeFrontSleepTime,
      fullWorkbenchSleepTime,
      noReadyURLsSleepTime
    ));

		largeFrontSleepTime = 0;
		fullWorkbenchSleepTime = 0;
		noReadyURLsSleepTime = 0;

		statsThread.emit();
		frontier.virtualizer.collectIf(.50, .75);
	}

	private void startHighCostStats() {
    final Thread thread = new Thread(statsThread, statsThread.getClass().getSimpleName());
    thread.start();
  }

  private void doPurgeCheck( final long now ) throws IOException {
    synchronized (schemeAuthority2VisitState) {
      for (VisitState visitState : schemeAuthority2VisitState.visitStates())
        if (visitState != null) {
          /* We've been scheduled for purge, or we have fetched at least a
           * URL but haven't seen a URL for a PURGE_DELAY interval. Note that in the second case
           * we do not modify schemeAuthority2Count, as we might encounter some more URLs for the
           * same visit state later, in which case we will create it again. */
          if (visitState.nextFetch == Long.MAX_VALUE || visitState.nextFetch != 0
            && visitState.nextFetch < now - PURGE_DELAY
            && visitState.isEmpty()
            && !visitState.acquired
            /*&& visitState.lastExceptionClass == null*/) {
            LOGGER.info((visitState.nextFetch == Long.MAX_VALUE ? "Purging " : "Purging by delay ") + visitState);
            // This will modify the backing array on which we are enumerating, but it won't be a serious problem.
            frontier.virtualizer.remove(visitState);
            schemeAuthority2VisitState.remove(visitState);
          } else {
            if (visitState.purgeRequired) {

              frontier.virtualizer.remove(visitState);
              frontier.distributor.schemeAuthority2VisitState.remove(visitState);
            }
          }
        }
    }
  }
}
