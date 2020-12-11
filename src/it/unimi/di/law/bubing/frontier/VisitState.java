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

import crawlercommons.robots.SimpleRobotRules;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.URLRespectsRobots;
import it.unimi.di.law.bubing.util.Util;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import org.apache.http.cookie.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.NoSuchElementException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

//RELEASE-STATUS: DIST

/** A class maintaining the current state of the visit of a specific scheme+authority. These states are further
 *  aggregated into {@linkplain WorkbenchEntry workbench entries}, according to their IP address; a visit state contains
 *  a reference to the corresponding {@link WorkbenchEntry}, after the resolution of the hostname by a {@link DNSThread} has
 *  taken place. Note, though, that the visit state may be temporarily removed from the entry (albeit still keeping a reference
 *  to it) when it becomes empty.
 *
 * <p>An instance of this class also records the {@link #nextFetch} available for its {@link #schemeAuthority},
 * a queue of path+queries and a cached robot filter. The {@link #nextFetch} field is used to implement
 * the {@link Delayed} interface.
 *
 * <p>Instances of this class are synchronized, and enqueue/dequeue operations can happen concurrently: this is
 * necessary because {@link #firstPath()} is called by a {@link FetchingThread},
 * {@linkplain #dequeue() dequeuing} is performed by a {@link ParsingThread},
 * whereas {@linkplain #enqueueCrawlRequest(byte[]) enqueing} is performed by the {@link Distributor}.
 *
 * <h2>Broken visit states</h2>
 *
 * <p>A visit state is declared broken when some exception (e.g., socket timeout, connection timeout etc.) takes
 * place when trying to download a URL coming from the {@link VisitState}, or even when the hostname of the {@link VisitState}
 * cannot be resolved by the DNS. The kind of exception that took place is stored in {@link #lastExceptionClass} and a
 * {@linkplain #retries counter} is kept of the number of attempts performed so far. Every attempt causes an increase in
 * the interval that must elapse before trying again: the starting interval is determined by {@link ParsingThread#EXCEPTION_TO_WAIT_TIME},
 * and then it is doubled every time, until the number of attempts exceeds a fixed maximum ({@link ParsingThread#EXCEPTION_TO_MAX_RETRIES}).
 *
 * <p>Two things can happen:
 * 	<ul>
 * 		<li>when the number of attempts exceeds the maximum, two different behaviors are possible, depending on {@link ParsingThread#EXCEPTION_HOST_KILLER}:
 *      if the exception is not very serious, the URL that caused the exception is thrown away and the next one is attempted (the
 * 		visit state is not broken any more, at this point, unless an exception takes place with the new URL); for serious exceptions,
 * 		the visit state is <em>scheduled for purge</em>: it gets emptied, the number of URLs fetched is set to {@link Integer#MAX_VALUE} to
 * 	    avoid downloading more pages, and eventually all the URLs found on disk for that host will also be discarded;
 * 		<li>before the limit has reached, either the page is finally downloaded successfully, or another (different) exception happens,
 * 		which maintains the visit state in its broken condition, but the situation is reset <em>as if</em> the state has just now
 * 	    broken.
 *   </ul>
 *
 */
public class VisitState implements Delayed, Serializable {
	private static final Logger LOGGER = LoggerFactory.getLogger(VisitState.class);
	private static final long serialVersionUID = 1L;

	/** A special path marking a <code>robots.txt</code> refresh request. */
	public final static byte[] ROBOTS_PATH = PulsarHelper.toMinimalCrawlRequestSerialized(PulsarHelper.minimalCrawlRequestFromPathQuery( "/robots.txt" ));

	/** A singleton empty cookie array. */
	public final static Cookie[] EMPTY_COOKIE_ARRAY = {};

	/** After this interval of time, we consider a host finished and we do not include it in the statistics. */
	private static final long DEATH_INTERVAL = TimeUnit.HOURS.toMillis(2);
	/** The window over which we compute the moving average. */
	public static final long MOVING_AVERAGE_WINDOW = TimeUnit.HOURS.toMillis(1);
	/** The scheme and authority visited by this visit state. */
	public final byte[] schemeAuthority;
	/** The minimum time at which this visit state can be accessed because of host-based politeness, zero at creation.
	 * If it is equal to {@link Long#MAX_VALUE}, the maximum number of URLs has been reached for this visit state. */
	public volatile long nextFetch;
	/** {@link System#currentTimeMillis()} when we fetched the robots we are {@linkplain #robotsFilter caching}. */
	public volatile long lastRobotsFetch;
	/** The robots-forbidden prefixes we are caching, as returned from the {@link URLRespectsRobots#parseRobotsReader(Reader, String,org.apache.commons.lang.mutable.MutableInt)} method. */
	public volatile SimpleRobotRules robotsFilter;
	/** The workbench entry this visit state belongs to. Note that this field is always
	 * non-{@code null}, regardless of whether this visit state is actually in the queue of
	 * its workbench entry, unless {@link #lastExceptionClass} is not {@code null}. */
	public transient volatile WorkbenchEntry workbenchEntry;
	/** The cookies of this visit state. */
	public Cookie[] cookies;
	/** If not {@code null}, this fields contains the class of the exception that was
	 * thrown during the last attempt to access this scheme+authority.
	 * If this happens, it might be the case that {@link #workbenchEntry} is {@code null}. */
	public volatile Class<? extends Throwable> lastExceptionClass;
	/** If {@link #lastExceptionClass} is not {@code null}, the count of the retries for this type of exception. */
	public volatile int retries;
	/** Whether this visit state is currently {@linkplain Workbench#acquire() acquired}. */
	protected volatile transient boolean acquired;
	/** Whether this visit state is currently {@linkplain Workbench#acquire() scheduled for purge}. */
	protected volatile transient boolean purgeRequired;
	/** The path+queries that must be visited for this visit state. */
	private final transient ObjectArrayFIFOQueue<byte[]> crawlRequests;
	/** Number of pages fetched */
	public volatile int nbFetched;


	/** Specific crawl-delay */
	public int crawlDelayMS;
	/** Creates a visit state.
	 *
	 * @param schemeAuthority the scheme+authority of this {@link VisitState}.
	 */
	public VisitState(final byte[] schemeAuthority) {

		this.schemeAuthority = schemeAuthority;
		cookies = EMPTY_COOKIE_ARRAY;
		crawlRequests = new ObjectArrayFIFOQueue<>();
		purgeRequired = false;
		crawlDelayMS = 0;
		nbFetched = 0;
	}


	/** Sets the workbench entry and put this visit state in its entry if it is nonempty.
	 *
	 * <ul>
	 * <li>Preconditions: not {{@link #acquired}}.
	 * </ul>
	 */
	public synchronized void setWorkbenchEntry(final WorkbenchEntry workbenchEntry) {
		assert ! acquired : this;
		this.workbenchEntry = workbenchEntry;
		if (! isEmpty()) workbenchEntry.add(this, Agent.getFrontier().workbench);
	}

	/** Puts this visit state in its entry, if it is not acquired and it has a non-{@code null} {@link #workbenchEntry}.
	 *
	 * <p>This method must be called in a synchronized section.
	 *
	 * <ul>
	 * <li>Preconditions: not {@link #isEmpty()}.
	 * </ul>
	 */
	private void putInEntryIfNotAcquired() {
		assert ! isEmpty() : this;
		Frontier frontier = Agent.getFrontier();
		if (! acquired && workbenchEntry != null) workbenchEntry.add(this, frontier.workbench);
	}

	/** Puts this visit state in its entry, if it not empty.
	 *
	 * <p>This method is called only by {@link Workbench#release(VisitState)}.
	 * Note that the associated entry is <em>not</em> put back on the workbench. Use
	 * {@link WorkbenchEntry#putOnWorkbenchIfNotEmpty(Workbench)} for that purpose.
	 *
	 * <ul>
	 * <li>Preconditions: {@link #workbenchEntry} &ne; {@code null}, {@link #acquired}.
	 * <li>Postconditions: not {@link #acquired}.
	 * </ul>
	 */
	public synchronized void putInEntryIfNotEmpty() {
		assert workbenchEntry != null : this;
		assert acquired : this;
		assert workbenchEntry.acquired : workbenchEntry;
		if (! isEmpty()) workbenchEntry.add(this);
		acquired = false;
	}

	/** Enqueues the <code>/robots.txt</code> path as the first element of the queue, if the queue is
	 * empty, or as the second element otherwise, possibly putting this visit state in its
	 * entry.
	 *
	 * <p>This behaviour is necessary: is this visit state
	 * is {@linkplain #acquired}, the first path+query might be still need to be dequeued by a
	 * {@link ParsingThread}.
	 */
	public synchronized void enqueueRobots() {
		if (! RuntimeConfiguration.FETCH_ROBOTS) return;
		if (nextFetch == Long.MAX_VALUE) return;
		synchronized(this) {
			if (crawlRequests.isEmpty()) {
				crawlRequests.enqueueFirst(ROBOTS_PATH);
				putInEntryIfNotAcquired();
			}
			else {
				final byte[] first = crawlRequests.dequeue();
				crawlRequests.enqueueFirst(ROBOTS_PATH);
				crawlRequests.enqueueFirst(first);
			}
		}
	}

	/** Forcibly enqueues the <code>/robots.txt</code> path as the first element of the queue.
	 *
	 *  <p>This method is useful only when restoring a saved state.
	 */
	public synchronized void forciblyEnqueueRobotsFirst() {
		if (! RuntimeConfiguration.FETCH_ROBOTS) return;
		crawlRequests.enqueueFirst(ROBOTS_PATH);
	}

	/** Remove the <code>/robots.txt</code> path, if present, and in this case sets the last robots fetch
	 * to {@link Long#MAX_VALUE}. Note that <strong>this method is not synchronized</strong> and
	 * should be called only after having stopped all threads accessing this visit state. */
	void removeRobots() {
		// Nothing to remove
		if (crawlRequests.isEmpty()) return;

		// We test the first path
		final byte[] firstPath = crawlRequests.dequeue();
		if (firstPath == VisitState.ROBOTS_PATH) {
			// It's robots.txt: we set the last robots fetch time and don't put it back
			lastRobotsFetch = Long.MAX_VALUE;
			return;
		}

		// It's not robots.txt
		if (isEmpty()) {
			// If there are no more paths, we put it back and return
			crawlRequests.enqueueFirst(firstPath);
			return;
		}

		// We test the second path
		final byte[] secondPath = crawlRequests.dequeue();
		if (secondPath == VisitState.ROBOTS_PATH) {
			// It's robots.txt: we put back the first path and set the last robots fetch time (and don't put back the second path)
			crawlRequests.enqueueFirst(firstPath);
			lastRobotsFetch = Long.MAX_VALUE;
			return;
		}

		// No robots.txt found: we put back everything
		crawlRequests.enqueueFirst(secondPath);
		crawlRequests.enqueueFirst(firstPath);
	}

	/** Enqueues a path+query in byte-array representation, possibly putting this visit state in its
	 * entry.
	 *
	 * <p>Note that if you enqueue more than once the same URL to a visit state using
	 * this method, it will be fetched twice.
	 *
	 * <p>Note that the scheme+authority of <code>url</code> must be {@link #schemeAuthority}.
	 *
	 * @param crawlRequestBytes a path+query in byte-array representation.
	 */
	public void enqueueCrawlRequest(final byte[] crawlRequestBytes) {
		synchronized (this) {
			if (nextFetch == Long.MAX_VALUE) return;
			final boolean wasEmpty = this.crawlRequests.isEmpty();
			this.crawlRequests.enqueue(crawlRequestBytes);
			if (wasEmpty) putInEntryIfNotAcquired();
		}
		Frontier frontier = Agent.getFrontier();
		frontier.pathQueriesInQueues.incrementAndGet();
		frontier.weightOfpathQueriesInQueues.addAndGet(BURL.memoryUsageOf(crawlRequestBytes));
	}

	/** Peeks at the first path in the queue.
	 *
	 * <p>The result of this call should be passed to {@link BURL#fromNormalizedSchemeAuthorityAndPathQuery(String, byte[])}
	 * to obtain the final BUbiNG URL.
	 *
	 * @return the first path in the queue.
	 * @throws NoSuchElementException if the queue of path+queries is empty.
	 */
	public synchronized byte[] firstPath() {
		return crawlRequests.first();
	}

	/** Removes the first path in the queue.
	 *
	 * @return the first path in the queue.
	 * @throws NoSuchElementException if the queue of path+queries is empty.
	 */
	public byte[] dequeue() {
		final byte[] array;
		synchronized (this) {
			array = crawlRequests.dequeue();
		}
		Frontier frontier = Agent.getFrontier();
		if (array != ROBOTS_PATH) {
			frontier.pathQueriesInQueues.decrementAndGet();
			frontier.weightOfpathQueriesInQueues.addAndGet(-BURL.memoryUsageOf(array));
		}

		return array;
	}

	public void setCrawlDelayMS(int crawlDelayMS) {
		this.crawlDelayMS = crawlDelayMS;
	}

	/** Empties this visit state of all the URLs that it contains. */
	public synchronized void clear() {
		while(! isEmpty()) dequeue(); // We cannot invoke crawlRequests.clear(), as counters would not be updated.
		// TODO: we should find a way to release more resources.
		crawlRequests.trim();
	}

	/** Computes the size (i.e., number of URLs) in this visit state.
	 *
	 * @return the size.
	 */
	public synchronized int size() {
		return crawlRequests.size();
	}

	/** Returns whether this visit state is empty.
	 *
	 * @return <code>true</code> iff this visit state does not contain any URL.
	 */
	public synchronized boolean isEmpty() {
		return crawlRequests.isEmpty();
	}

	/** Return whether this visit state is fetchable (i.e., if there is at leas one URL and it is allowed by politeness to fetch it).
	 *
	 * @param time the current time.
	 * @return <code>true</code> if the state is fecthable
	 */
	public boolean isAlive(final long time) {
		return size() != 0 || nextFetch >= time - DEATH_INTERVAL;
	}

	@Override
	public String toString() {
		return "[" + Util.toString(schemeAuthority) + " (" + size() + (lastExceptionClass != null ? ", " + lastExceptionClass.getSimpleName() : "") + ")]";
	}

	@Override
	public long getDelay(final TimeUnit unit) {
		return unit.convert(Math.max(0, nextFetch - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
	}

	@Override
	public int compareTo(final Delayed o) {
		return Long.signum(nextFetch - ((VisitState)o).nextFetch);
	}

	/** Disables permanently this visit state and schedules its purge
	 * by setting the {@linkplain Frontier#schemeAuthority2Count count} associated
	 * with its {@link #schemeAuthority} to {@link Integer#MAX_VALUE}, {@linkplain #clear() clearing} the internal queue and
	 * setting {@link #nextFetch} to {@link Long#MAX_VALUE}.
	 */
	public synchronized void schedulePurge() {
		assert acquired || workbenchEntry == null : acquired + " " + workbenchEntry;
		this.purgeRequired = true;
		nextFetch = Long.MAX_VALUE;
		clear();
	}

	/** Checks whether the current robots information has expired and, if necessary, schedules a new <code>robots.txt</code> download.
	 *
	 * @param time the current time.
	 */
	public void checkRobots(final long time) {
		if (! RuntimeConfiguration.FETCH_ROBOTS) return;

		/* Safeguard: if there's no robots filter, and I'm not fetching it, and it's not the first
		 * path in the queue, then something's wrong. */
		if (robotsFilter == null && lastRobotsFetch != Long.MAX_VALUE && (isEmpty() || firstPath() != ROBOTS_PATH)) {
			LOGGER.error("No robots filter and no robots path for " + this);
			lastRobotsFetch = Long.MAX_VALUE; // This inhibits further enqueueing until robots.txt is fetched.
			enqueueRobots();
			return;
		}

		if (lastExceptionClass == null) {
			// If we are not retrying an exception, we check whether it is time to reload robots.txt
			Frontier frontier = Agent.getFrontier();
			final long robotsExpiration = frontier.rc.robotsExpiration;
			if (time - robotsExpiration >= lastRobotsFetch) {
				if (robotsFilter == null) {
					if (lastRobotsFetch == 0) LOGGER.info("Going to get robots for {} for the first time", Util.toString(schemeAuthority));
					else LOGGER.info("Going to try again to get robots for {}", Util.toString(schemeAuthority));
				}
				else LOGGER.info("Going to get robots for {} because it has expired ({} >= {} ms have passed)", Util.toString(schemeAuthority), Long.valueOf(time - lastRobotsFetch), Long.valueOf(robotsExpiration));
				lastRobotsFetch = Long.MAX_VALUE; // This inhibits further enqueueing until robots.txt is fetched.
				enqueueRobots();
			}
		}
	}

	/** Returns an estimate of the number of path+queries that this visit state should keep in memory.
	 *
	 * @return an estimate of the number of path+queries that this visit state should keep in memory.
	 */
	public int pathQueryLimit() {
		Frontier frontier = Agent.getFrontier();
		/* We first compute the ratio beween the delay for scheme+authorities and the IP delay.
		 * It is usually greater than one and it expresses the number of scheme+authorities
		 * that can "fit" the IP delay. */
		final double delayRatio = Math.max(1 , (frontier.rc.schemeAuthorityDelay + 1.) / (frontier.rc.ipDelay + 1.));
		/* We now establish a scaling factor depending on the size of the workbench entry. Entries of
		 * size less than or equal to delayRatio have scaling factor one, as there is no scheme+authority
		 * slowdown due to the IP delay. However, entries with greater size are scaled proportionally.
		 * For instance, if delayRatio is 4 up to 4 scheme+authorities with the same IP on average will cause
		 * no slowdown, but if you have 8 scheme+authorities on the same IP each one will be slowed down by a factor
		 * of two: indeed, if scheme+authorities are accessed round-robin, you will need to wait for 8 ipDelays, which
		 * amounts to 2 scheme+authority delays. */
		final double scalingFactor = workbenchEntry == null ? 1 : Math.max(1, workbenchEntry.size() / delayRatio);
		/* Finally, we divide the estimated workbench size in path+queries by the size of the required front multiplied
		 * by the scaling factor. We also maximize with 4 and minimize with the number of path+queries that
		 * can be fetched in five minutes. */
		return (int)Math.min(300000 / (frontier.rc.schemeAuthorityDelay + 1),
				Math.max(4, (long)Math.ceil((frontier.workbenchSizeInPathQueries /
						(scalingFactor * frontier.requiredFrontSize.get())))));
	}

	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
		s.defaultWriteObject();
		int size = crawlRequests.size();
		s.writeInt(size);

		while(size-- != 0) Util.writeByteArray(crawlRequests.dequeue(), s);
	}

	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		s.defaultReadObject();
		int size = s.readInt();

		Field field = getClass().getDeclaredField("crawlRequests");
		field.setAccessible(true);
		field.set(this, new ObjectArrayFIFOQueue<byte[]>(size));

		while(size-- != 0) crawlRequests.enqueue(Util.readByteArray(s));
	}
}
