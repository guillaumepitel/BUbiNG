package it.unimi.di.law.bubing.frontier;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.hadoop.compression.fourmc.ZstdCodec;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.comm.FetchInfoSendThread;
import it.unimi.di.law.bubing.frontier.comm.PulsarManager;
import it.unimi.di.law.bubing.util.*;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;
import net.htmlparser.jericho.Config;
import net.htmlparser.jericho.LoggerProvider;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Lookup;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

//RELEASE-STATUS: DIST

/** The BUbiNG frontier: a class structure that encompasses most of the logic behind the way BUbiNG
 * fetches URLs. To understand how this class works, we invite the reader to consult the
 * documentation of {@link VisitState} first.
 *
 * <p>Every BUbiNG agent contains an instance of this class, which is responsible for starting and
 * orchestrating the mutual interaction of many different elements: <ul> <li>the
 * {@linkplain #workbench workbench} (described below), that contains the {@link VisitState}s to be
 * visited next; <li>the {@link Distributor}, that fills the workbench, possibly virtualizing part
 * of it through a {@link WorkbenchVirtualizer}; <li>the {@link TodoThread} and the
 * {@link DoneThread}, that move around {@link VisitState}s during the crawl; <li>a number of worker
 * threads doing useful work, like {@link DNSThread}, {@link FetchingThread} and
 * {@link ParsingThread}. </ul>
 *
 * <h2>How the frontier handles URLs</h2>
 *
 * <p>URLs in BUbiNG belong to one of two states:
 *
 * <ul>
 *
 * <li><em>received</em>: the URL has come from the ouside frontier
 *
 * <li><em>visited</em>: the URL became ready and was somehow handled (e.g., fetched and possibly
 * stored).
 *
 * </ul>
 *
 * <p>Note that waiting URLs that happen to be ready or visited will never come out of the sieve: it
 * is the logic of the sieve itself that inhibits the same object to be emitted twice.
 *
 * <p>All ready URLs are initially stored in a {@link ByteArrayDiskQueue} called {@link #receivedCrawlRequests},
 * from which they are moved to the FIFO queue of their {@link VisitState} by the
 * {@link Distributor}. Inside a {@link VisitState}, we only store a byte-array represention of the
 * path+query of ready URLs. Some of them may be stored outside of the visit state, through
 * {@linkplain WorkbenchVirtualizer virtualization}. Keeping path+queries in byte-array form in all
 * components reduces enormously object creation, and provides a simple form of compression by
 * prefix omission.
 *
 * <h2>The lifecycle of a URL</h2>
 *
 * <p>URLs are {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued to the frontier} either because
 * they are part of the visit seed, or because a {@link ParsingThread} has found them.
 * The method {@link #enqueue(com.exensa.wdl.protobuf.crawler.MsgCrawler.FetchInfo)} will
 * first check whether we have stored already too many URLs for the URL scheme+authority, in which
 * case the URL is discarded; then, it will check whether the URL already appears in the URL cache
 * (also in this case, the URL is discarded); finally it will check whether there is another agent
 * responsible for the URL, and in this case the URL will be sent to the appropriate agent using the
 * JAI4J infrastructure. If all checks have passed, the URL will be put in the sieve.
 *
 *
 * <p>Note that we expect that <em>the vast majority of URLs will be of the first kind</em>. This is
 * very important, as there is much less contention on visit state locks than on the frontier lock. */
public class Frontier {
	private static final Logger LOGGER = LoggerFactory.getLogger(Frontier.class);

	public static enum PropertyKeys {
		PATHQUERIESINQUEUES,
		WEIGHTOFPATHQUERIESINQUEUES,
		BROKENVISITSTATES,
		NUMBEROFRECEIVEDURLS,
		REQUIREDFRONTSIZE,
		FETCHINGTHREADWAITS,
		FETCHINGTHREADWAITINGTIMESUM,
		ARCHETYPESOTHERS,
		ARCHETYPES1XX,
		ARCHETYPES2XX,
		ARCHETYPES3XX,
		ARCHETYPES4XX,
		ARCHETYPES5XX,
		DUPLICATES,
		FETCHEDRESOURCES,
		FETCHEDROBOTS,
		TRANSFERREDBYTES,
		AVERAGESPEED,
		// ALERT: we should keep also:
		// outDegree, outHostDegree, contentLength, contentTypeText, contentTypeImage,
		// contentTypeApplication, contentTypeOthers;
		// EXTRAS
		CURRENTQUEUE,
		VIRTUALQUEUESIZES,
		VIRTUALQUEUESBIRTHTIME,
		READYURLSSIZE,
		RECEIVEDURLSSIZE,
		HOLDBACKURLSSIZE,
		// DISTRIBUTOR EXTRAS
		DISTRIBUTORWARMUP,
		DISTRIBUTORVISITSTATESONDISK,
		// METADATA
		EPOCH,
		CRAWLDURATION,
		VISITSTATESETSIZE,
		WORKBENCHENTRYSETSIZE
	};

	/** The loopback address, cached. */
	public static final InetAddress[] LOOPBACK;

	static {
		try {
			LOOPBACK = new InetAddress[] { InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }) };
		} catch (UnknownHostException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	/** The name of the file in {@link RuntimeConfiguration#storeDir} storing downloaded
	 * <code>robots.txt</code> files. */
	private static final String ROBOTS_STORE = "robots.warc.gz";

	/** The increase of the front size used by {@link #updateRequestedFrontSize()}. */
	private static final long FRONT_INCREASE = 250;

	/** Minimum number of available fetchdata required before we decide to drop one */
	final AtomicInteger fetchDataCount = new AtomicInteger(0);

	/** A hash function using {@link MurmurHash3}. */
	public final static AbstractHashFunction<byte[]> BYTE_ARRAY_HASHING_STRATEGY = new AbstractHashFunction<byte[]>() {
		private static final long serialVersionUID = 1L;

		@Override
		public long getLong(final Object key) {
			return MurmurHash3.hash((byte[])key);
		}
	};

	/** A hash function using {@link MurmurHash3}. */
	public final static AbstractHashFunction<ByteArrayList> BYTE_ARRAY_LIST_HASHING_STRATEGY = new AbstractHashFunction<ByteArrayList>() {
		private static final long serialVersionUID = 1L;

		@Override
		public long getLong(final Object key) {
			return MurmurHash3.hash((ByteArrayList)key);
		}
	};

	/** The agent that created this frontier. */
	protected final Agent agent;

	/** The runtime configuration. */
	public final RuntimeConfiguration rc;

	/** The manager for pulsar consumers and producers */
	public final PulsarManager pulsarManager;

	/** A queue to quickly buffer Outgoing Discovered URLs that will be submitted to pulsar. */
	public final ArrayBlockingQueue<MsgCrawler.FetchInfo> fetchInfoSendQueue;

	/** A queue to quickly buffer URLs to be crawled. */
	public final ArrayBlockingQueue<MsgFrontier.CrawlRequest> receivedCrawlRequests;

	/** The parsing threads. */
	public final ObjectArrayList<ParsingThread> parsingThreads;

	/** Thread activity metrics. */
	public final AtomicInteger runningParsingThreads;
	public final AtomicInteger runningFetchingThreads;
	public final AtomicInteger runningDnsThreads;

	public final AtomicInteger workingParsingThreads;
	public final AtomicInteger workingFetchingThreads;
	public final AtomicInteger workingDnsThreads;


	/** The Warc file where to write (if so requested) the downloaded <code>robots.txt</code> files. */
	public final ThreadLocal<WarcWriter> robotsWarcParallelOutputStream;

	/** The overall number of path+queries stored in {@link VisitState} queues. */
	public final AtomicLong pathQueriesInQueues;
	public final AtomicLong pathQueriesInDiskQueues;

	/** The overall {@linkplain BURL#memoryUsageOf(byte[]) memory} (in bytes) used by path+queries
	 * stored in {@link VisitState} queues. */
	public final AtomicLong weightOfpathQueriesInQueues;

	/** The number of broken {@linkplain VisitState visit states}. */
	// TODO: maybe we can eliminate this--it is used only in stats, where it can be recomputed
	public final AtomicLong brokenVisitStates;

	/** The overall number of URLs sent by other agents. */
	public final AtomicLong numberOfReceivedURLs;
	public final AtomicLong numberOfSentURLs;

	/** The workbench. */
	public final Workbench workbench;

	/** The queue of unknown hosts. */
	public final DelayQueue<VisitState> unknownHosts;

	/** The queue of new {@linkplain VisitState visit states}; filled by the
	 * {@linkplain #distributor} and emptied by the {@linkplain #dnsThreads DNS threads}. */
	public final LinkedBlockingQueue<VisitState> newVisitStates;

	private final FetchInfoSendThread fetchInfoSendThread;

	/** The threads resolving DNS for new {@linkplain VisitState visit states}. */
	protected final ObjectArrayList<DNSThread> dnsThreads;

	/** The threads fetching data. */
	private final ObjectArrayList<FetchingThread> fetchingThreads;

	/** The thread constantly moving ready URLs into the {@linkplain #workbench}. */
	protected final Distributor distributor;

	/** The workbench virtualizer used by this frontier. */
	protected final WorkbenchVirtualizer virtualizer;

	/** A lock-free list of visit states ready to be visited; it is filled by the {@link TodoThread}
	 * and emptied by the {@linkplain FetchingThread fetching threads}. */
	public final LockFreeQueue<VisitState> todo;

	/** A lock-free list of visit states ready to be released; it is filled by
	 * {@linkplain FetchingThread fetching threads} and emptied by the {@link DoneThread}. */
	public final LockFreeQueue<VisitState> done;

	/** A queue of visit states ready to be reilled; it is filled by {@linkplain DoneThread fetching
	 * threads} and emptied by the {@link Distributor}. */
	public final LockFreeQueue<VisitState> refill;

	/** The current estimation for the size of the front in IP addresses. It is adaptively increased
	 * when a {@link FetchingThread} has to wait to retrieve a {@link VisitState} from the
	 * {@link #todo} queue. It is never more than half the {@linkplain #workbenchSizeInPathQueries
	 * number of path+queries that the workbench can hold}. */
	public final AtomicLong requiredFrontSize;

	/** The number of waits performed by fetching threads; every time the statistics are printed this
	 * value is reset. */
	public final AtomicLong fetchingThreadWaits;
	/** The sum of the waiting time of the waiting fetching threads: every time a fetching thread
	 * waits this sum is updated; every time the statistics are printed this value is reset. */
	public final AtomicLong fetchingThreadWaitingTimeSum;

	/** A lock-free list of {@link FetchData} to be parsed; it is filled by
	 * {@linkplain FetchingThread fetching threads} and emptied by the {@linkplain ParsingThread
	 * parsing threads}. */
	public final LockFreeQueue<FetchData> results;
	public final LockFreeQueue<FetchData> availableFetchData;

	/** The thread moving {@linkplain VisitState visit states} ready to be visited from the
	 * {@linkplain #workbench} to the {@link #todo} queue. */
	private TodoThread todoThread;

	/** The thread moving {@linkplain VisitState visit states} that have been visited from the
	 * {@link #done} queue to the {@linkplain #workbench}. */
	private DoneThread doneThread;

	/** In position <var>i</var>, with 0 &lt; <var>i</var> &lt;6, the number of pages stored (does
	 * not include duplicates) having status <var>i</var>xx. In position 0, the number of pages with
	 * other status. */
	public final AtomicLong[] archetypesStatus;

	/** Statistics about the number of out-links of each archetype */
	public final SummaryStats outdegree;

	/** Statistics about the number of out-links of each archetype, without considering the links to
	 * the same corresponding host */
	public final SummaryStats externalOutdegree;

	/** Statistic about the content length of each archetype */
	public final SummaryStats contentLength;

	/** Number of archetypes whose indicated content type starts with text (case insensitive) **/
	public final AtomicLong contentTypeText;

	/** Number of archetypes whose indicated content type starts with image (case insensitive) **/
	public final AtomicLong contentTypeImage;

	/** Number of archetypes whose indicated content type starts with application (case insensitive) **/
	public final AtomicLong contentTypeApplication;

	/** Number of archetypes whose indicated content type does not start with text, image, or
	 * application (case insensitive) **/
	public final AtomicLong contentTypeOthers;

	/** The number of duplicate pages. */
	public final AtomicLong duplicates;

	/** The number of fetched resources (updated by {@link ParsingThread} instances). */
	public final AtomicLong fetchedResources;

	/** The number of fetched <code>robots.txt</code> files (updated by {@link ParsingThread}
	 * instances). */
	public final AtomicLong fetchedRobots;

	/** The overall number of transferred bytes. */
	public final AtomicLong transferredBytes;

	/** The logarithmically binned statistics of download speed in bits/s. */
	public final AtomicLongArray speedDist;
	public final AtomicLong fetchingDurationTotal;
	public final AtomicLong fetchingCount;
	public final AtomicLong fetchingFailedCount;
	public final AtomicLong fetchingFailedHostCount;
	public final AtomicLong fetchingFailedRobotsCount;
	public final AtomicLong fetchingRobotsCount;
	public final AtomicLong fetchingTimeoutCount;

	public final AtomicLong parsingDurationTotal;
	public final AtomicLong parsingCount;
	public final AtomicLong parsingRobotsCount;
	public final AtomicLong parsingErrorCount;
	public final AtomicLong parsingExceptionCount;


	/** The number of received new visit states (i.e. new hosts) */
	public final AtomicLong receivedVisitStates;

	/** The number of successfully resolved new visit states (i.e. new hosts resolved) */
	public final AtomicLong resolvedVisitStates;

	/** The average speeds of all visit states. */
	protected double averageSpeed;
	/** An estimation of the number of path+query objects that the workbench can store. */
	public volatile long workbenchSizeInPathQueries;
	/** The default configuration for a non-<code>robots.txt</code> request. */
	public RequestConfig defaultRequestConfig;
	/** The configuration for a no redirect request. */
	public RequestConfig noRedirectRequestConfig;
	/** The default configuration for a <code>robots.txt</code> request. */
	public RequestConfig robotsRequestConfig;

	private void setNoRedirectRequest() {
		noRedirectRequestConfig = RequestConfig.custom()
			.setSocketTimeout(rc.socketTimeout)
			.setConnectTimeout(rc.connectionTimeout)
			.setConnectionRequestTimeout(rc.connectionTimeout)
			.setCookieSpec(rc.cookiePolicy)
			.setRedirectsEnabled(false)
			.setContentCompressionEnabled(true)
			.setProxy(rc.proxyHost.length() > 0 ? new HttpHost(rc.proxyHost, rc.proxyPort) : null)
			.build();
		LOGGER.info("Set default request config to {}", defaultRequestConfig.toString());
	}

	private void setDefaultRequest() {
		defaultRequestConfig = RequestConfig.custom()
			.setSocketTimeout(rc.socketTimeout)
			.setConnectTimeout(rc.connectionTimeout)
			.setConnectionRequestTimeout(rc.connectionTimeout)
			.setCookieSpec(rc.cookiePolicy)
			.setRedirectsEnabled(true)
			.setRelativeRedirectsAllowed(true)
			.setCircularRedirectsAllowed(true) // allow for cookie-based redirects
			.setMaxRedirects(10) // 2xGoogle's policy
			.setContentCompressionEnabled(true)
			.setProxy(rc.proxyHost.length() > 0 ? new HttpHost(rc.proxyHost, rc.proxyPort) : null)
			.build();
		LOGGER.info("Set default request config to {}", defaultRequestConfig.toString());
	}

	private void setRobotsRequest() {
		robotsRequestConfig = RequestConfig.custom()
			.setSocketTimeout(rc.socketTimeout)
			.setConnectTimeout(rc.connectionTimeout)
			.setConnectionRequestTimeout(rc.connectionTimeout)
			.setCookieSpec(rc.cookiePolicy)
			.setRedirectsEnabled(true)
			.setRelativeRedirectsAllowed(true)
			.setCircularRedirectsAllowed(true) // allow for cookie-based redirects
			.setMaxRedirects(10) // 2xGoogle's policy
			.setProxy(rc.robotProxyHost.length() > 0 ? new HttpHost(rc.robotProxyHost, rc.robotProxyPort) : null)
			.build();
		LOGGER.info("Set robots request config to {}", robotsRequestConfig.toString());
	}

	public void setRequests() {
		setDefaultRequest();
		setRobotsRequest();
		setNoRedirectRequest();
	}

	/** Creates the frontier.
	 *
	 * @param rc the configuration to be used to set all parameters.
	 * @param agent the BUbiNG agent possessing this frontier. */
	public Frontier( final RuntimeConfiguration rc, final Agent agent, final PulsarManager pulsarManager ) throws IllegalArgumentException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		this.rc = rc;
		this.agent = agent;
		this.pulsarManager = pulsarManager;

		workbenchSizeInPathQueries = rc.workbenchMaxByteSize / 100;
		averageSpeed = 1. / rc.schemeAuthorityDelay;

		robotsWarcParallelOutputStream = null;
		if (false) {
			robotsWarcParallelOutputStream = new ThreadLocal<WarcWriter>() {
				@Override
				protected WarcWriter initialValue() {
					final File robotsFile = new File(rc.storeDir, "robots-" + UUID.randomUUID() + ".zstm");
					LOGGER.trace("Opening file " + robotsFile + " to write robots.txt");
					Configuration conf = new Configuration(true);
					CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
					String codecName = ZstdCodec.class.getName();
					CompressionCodec codec = ccf.getCodecByClassName(codecName);
					OutputStream warcOutputStream = null;
					if (codec == null)
						LOGGER.error("failed to load compression codec {}", codecName);
					else {
						try {
							warcOutputStream = new FastBufferedOutputStream(codec.createOutputStream(new FileOutputStream(robotsFile)), 1024 * 1024);
						} catch (IOException e) {
							LOGGER.error("Unable to create file", e);
							System.exit(1);
						}
					}
					WarcWriter warcWriter = new UncompressedWarcWriter(warcOutputStream);
					return warcWriter;
				}
			};
		}

		this.workbench = new Workbench();
		this.unknownHosts = new DelayQueue<>();
		this.virtualizer = new WorkbenchVirtualizer(this);

		receivedVisitStates = new AtomicLong();
		resolvedVisitStates = new AtomicLong();

		runningFetchingThreads = new AtomicInteger();
		runningParsingThreads = new AtomicInteger();
		runningDnsThreads = new AtomicInteger();
		workingFetchingThreads = new AtomicInteger();
		workingParsingThreads = new AtomicInteger();
		workingDnsThreads = new AtomicInteger();

		pathQueriesInQueues = new AtomicLong();
		pathQueriesInDiskQueues = new AtomicLong();

		weightOfpathQueriesInQueues = new AtomicLong();
		brokenVisitStates = new AtomicLong();
		fetchedResources = new AtomicLong();
		fetchedRobots = new AtomicLong();
		transferredBytes = new AtomicLong();
		speedDist = new AtomicLongArray(40);
		fetchingDurationTotal = new AtomicLong();
		fetchingCount = new AtomicLong();
		fetchingFailedCount = new AtomicLong();
		fetchingFailedRobotsCount = new AtomicLong();
		fetchingFailedHostCount = new AtomicLong();
		fetchingRobotsCount = new AtomicLong();
		fetchingTimeoutCount = new AtomicLong();
		parsingDurationTotal = new AtomicLong();
		parsingCount = new AtomicLong();
		parsingRobotsCount = new AtomicLong();
		parsingErrorCount = new AtomicLong();
		parsingExceptionCount = new AtomicLong();
		archetypesStatus = new AtomicLong[6];
		for (int i = 0; i < 6; i++) archetypesStatus[i] = new AtomicLong();
		outdegree = new SummaryStats();
		externalOutdegree = new SummaryStats();
		contentLength = new SummaryStats();
		contentTypeText = new AtomicLong();
		contentTypeImage = new AtomicLong();
		contentTypeApplication = new AtomicLong();
		contentTypeOthers = new AtomicLong();
		duplicates = new AtomicLong();
		numberOfReceivedURLs = new AtomicLong();
		numberOfSentURLs = new AtomicLong();
		requiredFrontSize = new AtomicLong(1500);
		fetchingThreadWaits = new AtomicLong();
		fetchingThreadWaitingTimeSum = new AtomicLong();

		setDefaultRequest();
		setRobotsRequest();
		setNoRedirectRequest();
		fetchInfoSendQueue = new ArrayBlockingQueue<>(  512);
		receivedCrawlRequests = new ArrayBlockingQueue<>( 4 * 1024);

		fetchInfoSendThread = new FetchInfoSendThread( pulsarManager, fetchInfoSendQueue);
		dnsThreads = new ObjectArrayList<>();
		fetchingThreads = new ObjectArrayList<>();
		parsingThreads = new ObjectArrayList<>();
		newVisitStates = new LinkedBlockingQueue<>();
		todo = new LockFreeQueue<>();
		done = new LockFreeQueue<>();
		refill = new LockFreeQueue<>();
		results = new LockFreeQueue<>();
		availableFetchData = new LockFreeQueue<>();

		distributor = new Distributor(this);

		// Configures Jericho to use SLF4J
		Config.LoggerProvider = LoggerProvider.SLF4J;
	}

	public void init() throws IOException, IllegalArgumentException, ConfigurationException, ClassNotFoundException, InterruptedException {
		if (rc.crawlIsNew) {
			distributor.statsThread.start(0);
		}

		// Never start child threads before every data structure is created or restored
		fetchInfoSendThread.start();
		distributor.start();
		(todoThread = new TodoThread(this)).start();
		(doneThread = new DoneThread(this)).start();

		// These must be coordinated with bind's settings, if any.
		Lookup.getDefaultCache(DClass.IN).setMaxEntries(rc.dnsCacheMaxSize);
		Lookup.getDefaultCache(DClass.IN).setMaxCache((int)Math.min(rc.dnsPositiveTtl, Integer.MAX_VALUE));
		Lookup.getDefaultCache(DClass.IN).setMaxNCache((int)Math.min(rc.dnsNegativeTtl, Integer.MAX_VALUE));
		Lookup.getDefaultResolver().setTimeout(Duration.ofMillis(rc.dnsTimeout));
	}

	/** Changes the number of DNS threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link DNSThread#stop} field.
	 *
	 * @param newDnsThreads the new number of threads. */
	public int dnsThreads(final int newDnsThreads) throws IllegalArgumentException {
		if (newDnsThreads <= 0) throw new IllegalArgumentException();

		synchronized (dnsThreads) {
			if (newDnsThreads < dnsThreads.size()) {
				for (int i = newDnsThreads; i < dnsThreads.size(); i++)
					dnsThreads.get(i).stop = true;
				dnsThreads.size(newDnsThreads);
				LOGGER.info("Number of DNS Threads set to " + dnsThreads.size());
				return dnsThreads.size();
			}

			for (int i = newDnsThreads - dnsThreads.size(); i-- != 0;) {
				final DNSThread thread = new DNSThread(this, dnsThreads.size());
				thread.start();
				dnsThreads.add(thread);
			}
		}

		LOGGER.info("Number of DNS Threads set to " + dnsThreads.size());
		return dnsThreads.size();
	}


	/** Changes the number of fetching threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link ParsingThread#stop} field.
	 *
	 * @param numFetchingThreads the new number of threads. */
	public int fetchingThreads(final int numFetchingThreads) throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
		if (numFetchingThreads < 0) throw new IllegalArgumentException();

		synchronized (fetchingThreads) {
			if (numFetchingThreads < fetchingThreads.size()) {
				// ALERT: add emergency interrupt
				for (int i = numFetchingThreads; i < fetchingThreads.size(); i++)
					fetchingThreads.get(i).stop = true;
				fetchingThreads.size(numFetchingThreads);
				LOGGER.info("Number of Fetching Threads set to " + fetchingThreads.size());
				return fetchingThreads.size();
			}

			for (int i = numFetchingThreads - fetchingThreads.size(); i-- != 0;) {
				final FetchingThread thread = new FetchingThread(this, fetchingThreads.size());
				thread.start();
				fetchingThreads.add(thread);
			}
		}

		LOGGER.info("Number of Fetching Threads set to " + fetchingThreads.size());
		return fetchingThreads.size();
	}

	/** Changes the number of parsing threads.
	 *
	 * <p>Note that when the number of thread is reduced, the stopped thread will actually terminate
	 * their execution as soon as they check the {@link ParsingThread#stop} field.
	 *
	 * @param newParsingThreads the new number of threads. */
	public int parsingThreads(final int newParsingThreads) throws IllegalArgumentException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, NoSuchAlgorithmException {
		if (newParsingThreads < 0) throw new IllegalArgumentException();

		synchronized (parsingThreads) {
			if (newParsingThreads < parsingThreads.size()) {
				for (int i = newParsingThreads; i < parsingThreads.size(); i++)
					parsingThreads.get(i).stop = true;
				parsingThreads.size(newParsingThreads);
				LOGGER.info("Number of Parsing Threads set to " + parsingThreads.size());
				return parsingThreads.size();
			}

			for (int i = newParsingThreads - parsingThreads.size(); i-- != 0;) {
				final ParsingThread thread = new ParsingThread(this, parsingThreads.size());
				thread.start();
				parsingThreads.add(thread);
			}
		}
		LOGGER.info("Number of Parsing Threads set to " + parsingThreads.size());
		return parsingThreads.size();
	}

	/** Enqueues a fetchInfo to be sent to the external Frontier.
	 *
	 *
	 * @param fetchInfo a {@linkplain MsgCrawler.FetchInfo Message} to be enqueued to the BUbiNG crawl. */
	public void enqueue( final MsgCrawler.FetchInfo fetchInfo ) {
		if ( fetchInfoSendQueue.remainingCapacity() == 0 )
			LOGGER.warn( "fetchInfoSendQueue is full" );
		fetchInfoSendQueue.offer( fetchInfo );
	}

	/** Returns whether the workbench is full.
	 *
	 * @return whether the workbench is full. */
	public boolean workbenchIsFull() {
		return weightOfpathQueriesInQueues.get() >= rc.workbenchMaxByteSize;
	}


	/** Closes the frontier: threads are stopped (if necessary, aborted), sieve and store and robots
	 * stream are closed.
	 *
	 * @throws IOException
	 * @throws InterruptedException */
	public void close() throws IOException, InterruptedException {
		/* First we wait for all high-level threads to complete. Note that only the workbench thread
		 * needs to be interrupted--all other threads check regularly for rc.stopping. Note that
		 * visit states in the todo and done list will be moved by snap() back into the workbench. */

		LOGGER.warn( "Closing Frontier" );

		LOGGER.warn( "Interrupting TODO thread" );
		todoThread.interrupt();

		LOGGER.warn( "Waiting distributor to stop" );
		distributor.join();
		LOGGER.warn( "Distributor stopped" );

		LOGGER.warn( "Waiting TODO thread to stop" );
		todoThread.join();
		LOGGER.warn( "TODO thread stopped" );

		/* First we stop DNS threads; note that we have to set explicitly stop. */
		LOGGER.warn( "Stopping DNS threads" );
		for (DNSThread t : dnsThreads) t.stop = true;
		LOGGER.warn( "Waiting for DNS threads to stop" );
		for (DNSThread t : dnsThreads) t.join();
		LOGGER.warn( "DNS threads stopped" );

		/* We wait for all fetching activity to come to a stop. */
		LOGGER.warn( "Stopping fetching threads" );
		for (FetchingThread t : fetchingThreads) t.stop = true;

		/* This extremely poor form of timeout waiting for fetching threads is motivated by threads
		 * hanging in ininterruptible, native socket I/O, and by the difficult to perform sensible
		 * joins with timeouts with thousands of threads. */
		long time = System.currentTimeMillis();
		boolean someAlive;

		do {
			someAlive = false;
			for (FetchingThread t : fetchingThreads) someAlive |= t.isAlive();
			if ( someAlive ) {
				LOGGER.warn( "Waiting fetching threads to stop" );
				Thread.sleep(1000);
			}
		} while (someAlive && System.currentTimeMillis() - time < rc.socketTimeout * 2);

		if (someAlive) {
			LOGGER.warn( "Aborting fetching threads" );
			for (FetchingThread t : fetchingThreads) t.abort(); // Abort any still open requests.
		}

		time = System.currentTimeMillis();
		do {
			someAlive = false;
			for (FetchingThread t : fetchingThreads) someAlive |= t.isAlive();
			if ( someAlive ) {
				LOGGER.warn( "Waiting fetching threads to abort" );
				Thread.sleep(1000);
			}
		} while (someAlive && System.currentTimeMillis() - time < rc.socketTimeout * 2);

		if (someAlive) {
			LOGGER.warn( "Interrupting fetching threads" );
			for (FetchingThread t : fetchingThreads) t.interrupt();
      // This catches fetching threads stuck because all parsing threads crashed
      LOGGER.warn( "Waiting fetching threads to interrupt" );
      for (FetchingThread t : fetchingThreads) t.join();
		}

		LOGGER.warn( "fetching threads stopped" );

		// We wait to be sure that the done thread wakes up and released all remaining visit states.
		LOGGER.warn( "Sleeping for 2s..." );
		Thread.sleep(2000);

		LOGGER.warn( "Stopping Done thread" );
		doneThread.stop = true;
		LOGGER.warn( "Waiting for Done thread to stop" );
		doneThread.join();
		LOGGER.warn( "Done thread stopped" );

		// Wait for all results to be parsed, unless there are no more parsing threads alive
		while (results.size() != 0) {
			someAlive = false;
			for (ParsingThread t : parsingThreads) someAlive |= t.isAlive();
			if ( !someAlive ) {
				LOGGER.error("No parsing thread alive: some results might not have been parsed");
				break;
			}
			LOGGER.warn( "Waiting for parsing threads to end" );
			Thread.sleep(1000);
		}
		if (results.size() == 0) LOGGER.info("All results have been parsed");

		/* Then we stop parsing threads; note that we have to set explicitly stop. */
		LOGGER.warn( "Stopping parsing threads" );
		for (ParsingThread t : parsingThreads) t.stop = true;
		LOGGER.warn( "Waiting parsing threads to stop" );
		for (ParsingThread t : parsingThreads) t.join();
		LOGGER.warn( "Parsing threads stopped" );

		//LOGGER.info("Joined parsing threads and closed stores");

		LOGGER.warn( "Stopping FetchInfoSendThread" );
		fetchInfoSendThread.stop = true;
		LOGGER.warn( "Waiting FetchInfoSendThread to stop" );
		fetchInfoSendThread.join();
		LOGGER.warn( "FetchInfoSendThread stopped" );

		LOGGER.warn( "Closing stores" );
		for (FetchingThread t : fetchingThreads) t.close();
		for (FetchData fd; (fd = availableFetchData.poll()) != null;) { fd.close(); }
		for (FetchData fd; (fd = results.poll()) != null;) { fd.close(); }
		LOGGER.warn( "Stores closed" );


		LOGGER.warn( "Releasing VisitStates" );
		// Move the todo list back into the workbench
		for (VisitState visitState; (visitState = todo.poll()) != null;) workbench.release(visitState);
		// Move the done list back into the workbench (here we catch visit states released by the interrupts on the fetching threads, if any)
		for (VisitState visitState; (visitState = done.poll()) != null;) {
			// We do not schedule for refill purged visit states
			if (visitState.nextFetch != Long.MAX_VALUE && virtualizer.count(visitState) > 0 && visitState.isEmpty()) refill.add(visitState);
			workbench.release(visitState);
		}

		// Fix all visit states in the refill queue
		for (VisitState visitState; (visitState = refill.poll()) != null;) {
			// Note that this might make temporarily the workbench too big by a little bit.
			final int dequeuedURLs = virtualizer.dequeueCrawlRequests(visitState, visitState.pathQueryLimit());
			if (dequeuedURLs == 0) LOGGER.info("No URLs on disk during last refill: " + visitState);
			if (visitState.acquired) LOGGER.warn("Visit state in the poll queue is acquired: " + visitState);
		}
		LOGGER.warn( "VisitStates released" );

		// We invoke done() here so the final stats are the last thing printed.
		distributor.statsThread.done();

		LOGGER.warn( "Frontier closed" );
	}

	public long getCurrentFrontSize() {
		return workbench.approximatedSize() + todo.size() - workbench.broken.get();
	}

		/** Update, if necessary, the {@link #requiredFrontSize}. The current front size is the number of
	 * visit states present in the workbench and in the {@link #todo} queue. If this quantity is
	 * larged than the {@linkplain #requiredFrontSize currently-required front size}, the latter is
	 * increase by {@link #FRONT_INCREASE}, although it will never be set to a value larger than
	 * half of the workbench (two queries per visit state). */
	public void updateRequestedFrontSize() {
		final long currentFrontSize = getCurrentFrontSize();
		final long currentRequiredFrontSize = requiredFrontSize.get();
		final long nextRequiredFrontSize = Math.min( currentRequiredFrontSize+FRONT_INCREASE, workbenchSizeInPathQueries/2 );
		// If compareAndSet() returns false the value has already been updated.
		if (currentFrontSize >= currentRequiredFrontSize && requiredFrontSize.compareAndSet(currentRequiredFrontSize,nextRequiredFrontSize) )
			LOGGER.info("Required front size: " + nextRequiredFrontSize);
	}

	/** Updates the statistics relative to the wait time of {@link FetchingThread}s.
	 *
	 * @param waitTime the length of a new pause by one of the fetching threads. */
	public void updateFetchingThreadsWaitingStats(final long waitTime) {
		fetchingThreadWaits.incrementAndGet();
		fetchingThreadWaitingTimeSum.addAndGet(waitTime);
	}

	/** Resets the statistics relative to the wait time of {@link FetchingThread}s. */
	public void resetFetchingThreadsWaitingStats() {
		//fetchingThreadWaits.set(0);
		//fetchingThreadWaitingTimeSum.set(0);
	}
	/** Returns the {@link StatsThread}.
	 *
	 * @return the stats thread. */
	public StatsThread getStatsThread() {
		return distributor.statsThread;
	}

	/** The number of pages stored (does not include duplicates).
	 *
	 * @return the number of pages stored. */
	public long archetypes() {
		long rst = 0;
		for (int i = 0; i < archetypesStatus.length; i++)
			rst += archetypesStatus[i].get();
		return rst;
	}
}
