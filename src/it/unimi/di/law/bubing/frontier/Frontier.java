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

import com.hadoop.compression.fourmc.ZstdCodec;
import it.unimi.di.law.bubing.Agent;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.categories.TextClassifier;
import it.unimi.di.law.bubing.frontier.comm.FetchInfoSendThread;
import it.unimi.di.law.bubing.frontier.comm.PulsarManager;
import it.unimi.di.law.bubing.util.*;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.jai4j.Job;
import it.unimi.dsi.jai4j.JobManager;
import it.unimi.dsi.stat.SummaryStats;
import it.unimi.dsi.sux4j.mph.AbstractHashFunction;
import it.unimi.dsi.util.Properties;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

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

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

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
 * <p>URLs in BUbiNG belong to one of four states:
 *
 * <ul>
 *
 * <li><em>ready</em>: the URL has come out of the sieve, so it will be visited at some point in the
 * future unless some limiting parameters (e.g.,
 * {@link RuntimeConfiguration#maxUrlsPerSchemeAuthority}) forbid it;
 *
 * <li><em>visited</em>: the URL became ready and was somehow handled (e.g., fetched and possibly
 * stored).
 *
 * </ul>
 *
 * <p>Note that waiting URLs that happen to be ready or visited will never come out of the sieve: it
 * is the logic of the sieve itself that inhibits the same object to be emitted twice.
 *
 * <p>All ready URLs are initially stored in a {@link ByteArrayDiskQueue} called {@link #quickReceivedCrawlRequests},
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
 * they are part of the visit seed, or because a {@link ParsingThread} has found them, or because
 * they have been {@linkplain JobManager#submit(Job) submitted using JAI4J} (this includes both
 * manual submission and URLs sent by other agents). The method {@link #enqueue(com.exensa.wdl.protobuf.crawler.MsgCrawler.FetchInfo)} will
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

	/** Names of the scalar fields saved by {@link #snap()}. */
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

	/** The minimum number of milliseconds between two flushes. */
	public static final long MIN_FLUSH_INTERVAL = 10000;

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

	/** An array of queues to quickly buffer discovered URLs that will have to be filtered and either dispatch or enqueued locally. */
	//public final ArrayBlockingQueue<MsgCrawler.FetchInfo> quickToQueueURLLists[];

	/** A queue to quickly buffer Outgoing Discovered URLs that will be submitted to pulsar. */
	public final ArrayBlockingQueue<MsgCrawler.FetchInfo> quickToSendDiscoveredURLs;

	/** A queue to quickly buffer URLs to be crawled. */
	public final ArrayBlockingQueue<MsgFrontier.CrawlRequest> quickReceivedCrawlRequests;

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

	/** The time at which the next flush can happen. */
	protected volatile long nextFlush;

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

	/** The URL cache. This cache stores the most recent URLs that have been
	 * {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued}. */
	//public final FastApproximateByteArrayCache urlCache; // FIXME: seive ?

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

	/** The global instance for the text classifier */
	public final TextClassifier textClassifier;

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
				if ( codec == null )
					LOGGER.error( "failed to load compression codec {}", codecName );
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

		this.textClassifier = rc.classifierClass.getConstructor(RuntimeConfiguration.class).newInstance(rc);

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
		quickToSendDiscoveredURLs = new ArrayBlockingQueue<>(  512);
		quickReceivedCrawlRequests = new ArrayBlockingQueue<>( 4 * 1024);

		fetchInfoSendThread = new FetchInfoSendThread( pulsarManager, quickToSendDiscoveredURLs );
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
		else {
			restore();
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

	/** Enqueues a URL to the the BUbiNG crawl.
	 *
	 * <ul>
	 *
	 * <li>if there are too many URLs for the URL scheme+authority, we discard the URL;
	 *
	 * <li>if the URL appears in the URL cache, we discard it; otherwise, we add it to the cache;
	 *
	 * <li>if another agent is responsible for the URL, we
	 * {@linkplain JobManager#submit(it.unimi.dsi.jai4j.Job) submit} it to the agent.
	 *
	 * </ul>
	 *
	 * @param fetchInfo a {@linkplain MsgCrawler.FetchInfo Message} to be enqueued to the BUbiNG crawl. */
	public void enqueue( final MsgCrawler.FetchInfo fetchInfo ) {
		if ( quickToSendDiscoveredURLs.remainingCapacity() == 0 )
			LOGGER.warn( "quickToSendDiscoveredURLs is full" );
		quickToSendDiscoveredURLs.offer( fetchInfo );
	}

	/*
	public void enqueue(final MsgCrawler.FetchInfo crawledPageInfo)  {

		try {
			if (LOGGER.isTraceEnabled()) LOGGER.trace("Sending out {}",
					crawledPageInfo.toString());
			quickToSendDiscoveredURLs.offer(crawledPageInfo);
		}
		catch (IllegalStateException e) {
			// This just shouldn't happen.
			LOGGER.warn("Impossible to submit URL " + crawledPageInfo.toString(), e);
		}

		return;
	}
	*/

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

	/** Snaps fields to files in the given directory. Fields that are of scalar are written into a
	 * single file named <code>frontier.data</code>. Other fields are written each in a file of its
	 * own, named with the name of the field. */
	public void snap() throws ConfigurationException, IllegalArgumentException, IOException {

		// Purge ROBOTS_PATH path+queries and set a fake last robots fetch so that ROBOTS_PATH will
		// be put back immediately after restart.
		for (VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) visitState.removeRobots();

		LOGGER.info("Final statistics");
		distributor.statsThread.emit();
		distributor.statsThread.run();

		final File snapDir = new File(rc.frontierDir, "snap");
		LOGGER.info("Started snapping to " + snapDir);
		if (snapDir.exists()) LOGGER.warn("Already existing snap directory " + snapDir + ": data will be overwritten (this shouldn't happen)");
		else if (!snapDir.mkdir()) {
			LOGGER.error("Could not create snap directory " + snapDir + ": will not produce snap");
			return;
		}

		LOGGER.info("Snapping scalar data");
		Properties scalarData = new Properties();
		final long epoch = System.currentTimeMillis();
		scalarData.addProperty(PropertyKeys.EPOCH, epoch);
		// TODO: make this locale-independent
		scalarData.setHeader("Snap started at " + new Date());

		// Scalar properties
		scalarData.addProperty(PropertyKeys.PATHQUERIESINQUEUES, pathQueriesInQueues.get());
		scalarData.addProperty(PropertyKeys.WEIGHTOFPATHQUERIESINQUEUES, weightOfpathQueriesInQueues.get());
		scalarData.addProperty(PropertyKeys.BROKENVISITSTATES, brokenVisitStates.get());
		scalarData.addProperty(PropertyKeys.NUMBEROFRECEIVEDURLS, numberOfReceivedURLs.get());
		scalarData.addProperty(PropertyKeys.REQUIREDFRONTSIZE, requiredFrontSize.get());
		scalarData.addProperty(PropertyKeys.FETCHINGTHREADWAITS, fetchingThreadWaits.get());
		scalarData.addProperty(PropertyKeys.FETCHINGTHREADWAITINGTIMESUM, fetchingThreadWaitingTimeSum.get());
		scalarData.addProperty(PropertyKeys.ARCHETYPESOTHERS, archetypesStatus[0].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES1XX, archetypesStatus[1].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES2XX, archetypesStatus[2].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES3XX, archetypesStatus[3].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES4XX, archetypesStatus[4].get());
		scalarData.addProperty(PropertyKeys.ARCHETYPES5XX, archetypesStatus[5].get());
		scalarData.addProperty(PropertyKeys.DUPLICATES, duplicates.get());
		scalarData.addProperty(PropertyKeys.FETCHEDRESOURCES, fetchedResources.get());
		scalarData.addProperty(PropertyKeys.FETCHEDROBOTS, fetchedRobots.get());
		scalarData.addProperty(PropertyKeys.TRANSFERREDBYTES, transferredBytes.get());
		scalarData.addProperty(PropertyKeys.AVERAGESPEED, averageSpeed);
		// scalarData.addProperty(PropertyKeys.DISTRIBUTORWARMUP, distributor.warmup);
		scalarData.addProperty(PropertyKeys.CRAWLDURATION, distributor.statsThread.requestLogger.millis());

		scalarData.addProperty(PropertyKeys.VISITSTATESETSIZE, distributor.schemeAuthority2VisitState.size());
		scalarData.addProperty(PropertyKeys.WORKBENCHENTRYSETSIZE, workbench.numberOfWorkbenchEntries());


		LOGGER.info("Storing virtualizer states");
		virtualizer.close();

		// readyURLs and receivedURLs
		LOGGER.info("Freezing byte disk queues");

		scalarData.save(new File(snapDir, "frontier.data"));

		LOGGER.info("Storing visit states");
		final ObjectOutputStream workbenchStream = new ObjectOutputStream(new FastBufferedOutputStream(new FileOutputStream(new File(snapDir, "workbench"))));

		long c = 0;
		for (VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) {
				if (visitState.acquired) LOGGER.error("Acquired visit state: " + visitState);
				c++;
			}

		workbenchStream.writeLong(c);

		for (VisitState visitState : distributor.schemeAuthority2VisitState.visitStates())
			if (visitState != null) {
				workbenchStream.writeObject(visitState);
				workbenchStream.writeBoolean(visitState.workbenchEntry != null);
				if (visitState.workbenchEntry != null) Util.writeByteArray(visitState.workbenchEntry.ipAddress, workbenchStream);
			}

		workbenchStream.close();
	}

	/** Restores data from the given directory.
         * @see #snap() */
	@SuppressWarnings("unchecked")
	public void restore() throws ConfigurationException, IllegalArgumentException, IOException, ClassNotFoundException {
		File snapDir = new File(rc.frontierDir, "snap");
		if (!snapDir.exists() || !snapDir.isDirectory()) {
			LOGGER.error("Trying to restore state from snap directory " + snapDir + ", but it does not exist or is not a directory");
			return;
		}

		LOGGER.info("Restoring data from " + snapDir);

		LOGGER.info("Restoring scalar data");
		Properties scalarData = new Properties(new File(snapDir, "frontier.data"));
		final long epoch = scalarData.getLong(PropertyKeys.EPOCH);

		// Scalar properties
		transferredBytes.set(scalarData.getLong(PropertyKeys.TRANSFERREDBYTES));
		pathQueriesInQueues.set(scalarData.getLong(PropertyKeys.PATHQUERIESINQUEUES));
		weightOfpathQueriesInQueues.set(scalarData.getLong(PropertyKeys.WEIGHTOFPATHQUERIESINQUEUES));
		brokenVisitStates.set(scalarData.getLong(PropertyKeys.BROKENVISITSTATES));
		numberOfReceivedURLs.set(scalarData.getLong(PropertyKeys.NUMBEROFRECEIVEDURLS));
		requiredFrontSize.set(scalarData.getLong(PropertyKeys.REQUIREDFRONTSIZE));
		fetchingThreadWaits.set(scalarData.getLong(PropertyKeys.FETCHINGTHREADWAITS));
		fetchingThreadWaitingTimeSum.set(scalarData.getLong(PropertyKeys.FETCHINGTHREADWAITINGTIMESUM));
		archetypesStatus[0].set(scalarData.getLong(PropertyKeys.ARCHETYPESOTHERS));
		archetypesStatus[1].set(scalarData.getLong(PropertyKeys.ARCHETYPES1XX));
		archetypesStatus[2].set(scalarData.getLong(PropertyKeys.ARCHETYPES2XX));
		archetypesStatus[3].set(scalarData.getLong(PropertyKeys.ARCHETYPES3XX));
		archetypesStatus[4].set(scalarData.getLong(PropertyKeys.ARCHETYPES4XX));
		archetypesStatus[5].set(scalarData.getLong(PropertyKeys.ARCHETYPES5XX));
		duplicates.set(scalarData.getLong(PropertyKeys.DUPLICATES));
		fetchedResources.set(scalarData.getLong(PropertyKeys.FETCHEDRESOURCES));
		fetchedRobots.set(scalarData.getLong(PropertyKeys.FETCHEDROBOTS));
		transferredBytes.set(scalarData.getLong(PropertyKeys.TRANSFERREDBYTES));
		averageSpeed = scalarData.getDouble(PropertyKeys.AVERAGESPEED);
		// distributor.warmup = scalarData.getBoolean(PropertyKeys.DISTRIBUTORWARMUP);

		distributor.schemeAuthority2VisitState.ensureCapacity(scalarData.getInt(PropertyKeys.VISITSTATESETSIZE));
		workbench.address2WorkbenchEntry.ensureCapacity(scalarData.getInt(PropertyKeys.WORKBENCHENTRYSETSIZE));

		/* LOGGER.info("Restoring virtualizer states and defreezing virtual queues");
		 * virtualizer.currentQueue = scalarData.getInt(PropertyKeys.CURRENTQUEUE); String[]
		 * virtualQueueSizes = scalarData.getStringArray(PropertyKeys.VIRTUALQUEUESIZES); final
		 * int numVirtualQueues = virtualQueueSizes.length; virtualizer.virtualQueue = new
		 * ByteArrayDiskQueue[numVirtualQueues]; virtualizer.virtualQueuesBirthTime =
		 * scalarData.getLong(PropertyKeys.VIRTUALQUEUESBIRTHTIME); for(int i = 0; i <
		 * numVirtualQueues; i++) { long virtualQueueSize = Long.parseLong(virtualQueueSizes[i]
		 *); virtualizer.virtualQueue[i] = virtualQueueSize == -1? null :
		 * WorkbenchVirtualizer.createOrOpenQueue(this, virtualizer.virtualQueuesBirthTime, i,
		 * numVirtualQueues, false, virtualQueueSize); } */

		LOGGER.info("Restoring workbench");
		final ObjectInputStream workbenchStream = new ObjectInputStream(new FastBufferedInputStream(new FileInputStream(new File(snapDir, "workbench"))));

		final long workbenchSize = workbenchStream.readLong();
		long w = workbenchSize;
		try {
			ThreadLocalRandom tlrng = ThreadLocalRandom.current();
			while(w-- != 0) {
				final VisitState visitState = (VisitState)workbenchStream.readObject();
				distributor.schemeAuthority2VisitState.add(visitState);
				final boolean nonNullWorkbenchEntry = workbenchStream.readBoolean();
				if (visitState.lastRobotsFetch == Long.MAX_VALUE) visitState.forciblyEnqueueRobotsFirst();

				if (nonNullWorkbenchEntry) {
					WorkbenchEntry entry = null;
					byte[] address = Util.readByteArray(workbenchStream);
					int overflowCounter = 0;
					long maxDelay = 0;
					do {
						entry = workbench.getWorkbenchEntry(address, overflowCounter);
						if (entry.size() > 0)
							maxDelay = Math.max(maxDelay, entry.delay);
						overflowCounter++;
					} while (entry.size() >= rc.maxInstantSchemeAuthorityPerIP * overflowCounter);
					entry.delay = maxDelay;
					if (entry.size() == 0) // it's a new one
						entry.delay = maxDelay + overflowCounter;
					visitState.setWorkbenchEntry(entry);
				}
				else
					newVisitStates.add(visitState);

				if (visitState.isEmpty() && virtualizer.count(visitState) > 0) {
					LOGGER.error("Empty visit state, URLs on disk: " + visitState);
					refill.add(visitState);
				}
			}
		}
		catch(EOFException e) {
			LOGGER.error("Workbench stream too short: " + w + " visit states missing out of " + workbenchSize);
		}
		workbenchStream.close();

		virtualizer.readMetadata();

		// readyURLs and receivedURLs
		LOGGER.info("Defreezing byte disk queues");

		// Move away snap directory, as its contents will become unsynchronized with queue data.
		final File renameDir = new File(snapDir + "-" + epoch);
		LOGGER.info("Renaming snap directory " + snapDir + " to " + renameDir);
		if (!snapDir.renameTo(renameDir)) LOGGER.error("Could not rename snap directory");

		// Starting stats
		distributor.statsThread.start(scalarData.getLong(PropertyKeys.CRAWLDURATION));
		LOGGER.info("Starting statistics");
		distributor.statsThread.emit();
		distributor.statsThread.run();
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
