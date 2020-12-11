package it.unimi.di.law.bubing;

import com.martiansoftware.jsap.*;
import it.unimi.di.law.bubing.frontier.Frontier;
import it.unimi.di.law.bubing.frontier.comm.PulsarManager;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.Link;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.di.law.warc.filters.parser.FilterParser;
import it.unimi.di.law.warc.filters.parser.ParseException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softee.management.annotation.Description;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;
import org.softee.management.exception.ManagementException;
import org.softee.management.helper.MBeanRegistration;
import org.xbill.DNS.Lookup;

import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.locks.Lock;

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

//RELEASE-STATUS: DIST

/** A BUbiNG agent. This class contains the main method used to start a BUbiNG agent, and exposes on JMX a number of methods
 * that expose internal settings. In many cases settings can be changed while BUbiNG is running. */

@MBean @Description("A BUbiNG agent")
public class Agent {
	private final static Logger LOGGER = LoggerFactory.getLogger(Agent.class);
	/** The name of the standard Java system property that sets the JMX service port (it must be set for the agent to start). */
	public static final String JMX_REMOTE_PORT_SYSTEM_PROPERTY = "com.sun.management.jmxremote.port";

	/** The only instance of global data in this agent. */
	private final RuntimeConfiguration rc;
	/** The frontier of this agent. */
	private final Frontier frontier;

	private final PulsarManager pulsarManager;

	private static Agent theAgent;
	private static volatile boolean hasStopped = false;

	protected final String name;
	protected final JMXServiceURL jmxServiceURL;
	protected final ObjectName selfObjectName;

	public Agent(final String hostname, final int jmxPort, final RuntimeConfiguration rc) throws Exception {
		// Initialize JMX related elements
		this.name = rc.name;
		final var jmxSocket = new InetSocketAddress(hostname, jmxPort);
		final String jmxServiceURLString = "service:jmx:rmi:///jndi/rmi://" + jmxSocket.getAddress().getHostAddress() + ":" + jmxSocket.getPort() + "/jmxrmi";
		this.jmxServiceURL = new JMXServiceURL(jmxServiceURLString);
		final String selfObjectNameString = this.getClass().getPackage().getName() + ":type=" + this.getClass().getSimpleName() + ",name=" + name;
		this.selfObjectName = new ObjectName(selfObjectNameString);

		theAgent = this;

		LOGGER.trace("Creating Agent instance with properties {}", rc);

		// TODO: check crawlIsNew for all components.
		this.rc = rc;

		register(); // Register the MBean

		pulsarManager = new PulsarManager( rc );
		pulsarManager.createFetchInfoProducers();

		frontier = new Frontier(rc, this, pulsarManager );
		frontier.init();

		// It is important that threads are allocated here, that is, after the agent has been connected.
		frontier.dnsThreads(rc.dnsThreads);
		frontier.parsingThreads(rc.parsingThreads);
		frontier.fetchingThreads(rc.fetchingThreads);

		pulsarManager.createCrawlRequestConsumers( frontier );
	}

	public static Agent getAgent() {
		return theAgent;
	}

	public static Frontier getFrontier() {
		return theAgent.frontier;
	}

	public void run() throws Exception {
		// We wait for the notification of a stop event, usually caused by a call to stop().
		frontier.rc.ensureNotPaused();

		synchronized (rc) {
			if (!rc.stopping)
				rc.wait();
		}

		terminate();
	}

	private void terminate() {
		synchronized ( this ) {
			if ( hasStopped )
				return;
			hasStopped = true;
		}

		unsafeTerminate();
	}

	private void unsafeTerminate() {
		try {
			// Stuff to be done at stopping time
			LOGGER.warn( "Terminating agent {}", this );

			synchronized ( rc ) {
				if ( !rc.stopping ) {
					LOGGER.warn( "Notifying termination to RuntimeConfiguration" );
					rc.stopping = true;
					rc.notifyAll();
				}
			}

			LOGGER.warn( "Closing CrawlRequest consumers" );
			pulsarManager.closeCrawlRequestConsumers();
			LOGGER.warn( "CrawlRequest consumers closed" );

			LOGGER.warn( "Closing Frontier" );
			frontier.close();
			LOGGER.warn( "Frontier closed" );

			LOGGER.warn( "Closing FetchInfo producers" );
			pulsarManager.closeFetchInfoProducers();
			LOGGER.warn( "FetchInfo producers closed" );

			LOGGER.warn( "Closing Pulsar client" );
			pulsarManager.closePulsarClient();
			LOGGER.warn( "Pulsar client closed" );

			LOGGER.warn( "Agent {} terminated", this );
		}
		catch ( InterruptedException e ) {
			LOGGER.error( String.format("Termination of agent %s was interrupted",this), e );
		}
		catch ( IOException e ) {
			LOGGER.error( String.format("While terminating agent %s",this), e );
		}
	}

	public void register() throws ManagementException {
		LOGGER.info("Registering " + this + "...");
		(new MBeanRegistration(this, this.selfObjectName)).register();
	}

	/* Main Managed Operations */

	@ManagedOperation @Description("Stop this agent")
	public void stop() {
		LOGGER.info("Going to stop the agent...");
		synchronized(rc) {
			rc.stopping = true;
			rc.notifyAll();
		}
	}

	@ManagedOperation @Description("Pause this agent")
	public void pause() {
		LOGGER.info("Going to pause the agent...");
		rc.paused = true;
	}

	@ManagedOperation @Description("Resume a paused agent")
	public void resume() {
		if (rc.paused) {
			LOGGER.info("Resuming the agent...");
			synchronized(rc) {
				rc.paused = false;
				rc.notifyAll();
			}
		}
		else LOGGER.warn("Agent not paused: not resuming");
	}

	@ManagedOperation @Description("Add a new IPv4 to the black list; it can be a single IP address or a file (prefixed by file:)")
	public void addBlackListedIPv4(@org.softee.management.annotation.Parameter("address") @Description("An IPv4 address to be blacklisted") String address) throws ConfigurationException, FileNotFoundException {
		final Lock lock = rc.blackListedIPv4Lock.writeLock();
		lock.lock();
		try {
			rc.addBlackListedIPv4(address);
		} finally {
			lock.unlock();
		}
	}

	@ManagedOperation @Description("Add a new host to the black list; it can be a single host or a file (prefixed by file:)")
	public void addBlackListedHost(@org.softee.management.annotation.Parameter("host") @Description("A host to be blacklisted") String host) throws ConfigurationException, FileNotFoundException {
		final Lock lock = rc.blackListedHostHashesLock.writeLock();
		lock.lock();
		try {
			rc.addBlackListedHost(host);
		} finally {
			lock.unlock();
		}
	}

	/* Properties, the same as RuntimeConfiguration: final fields in RuntimeConfiguration are not reported since they can be seen in the file .properties;
	 * while volatile fields in RuntimeConfiguration can be get and set (except for paused and stopping)
	 * */

	@ManagedAttribute
	public void setDnsThreads(final int dnsThreads) throws IllegalArgumentException {
		rc.dnsThreads = frontier.dnsThreads(dnsThreads);
	}

	@ManagedAttribute @Description("Number of DNS threads")
	public int getDnsThreads() {
		return rc.dnsThreads;
	}

	@ManagedAttribute
	public void setFetchingThreads(final int fetchingThreads) throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
		rc.fetchingThreads = frontier.fetchingThreads(fetchingThreads);
	}

	@ManagedAttribute @Description("Number of fetching threads")
	public int getFetchingThreads() {
		return rc.fetchingThreads;
	}

	@ManagedAttribute @Description("Number of running fetching threads")
	public int getRunningFetchingThreads() {
		return frontier.runningFetchingThreads.get();
	}

	@ManagedAttribute @Description("Number of working fetching threads")
	public int getWorkingFetchingThreads() {
		return frontier.workingFetchingThreads.get();
	}

	@ManagedAttribute @Description("Number of running parsing threads")
	public int getRunningParsingThreads() {
		return frontier.runningParsingThreads.get();
	}

	@ManagedAttribute @Description("Number of working parsing threads")
	public int getWorkingParsingThreads() {
		return frontier.workingParsingThreads.get();
	}

	@ManagedAttribute @Description("Number of running DNS threads")
	public int getRunningDnsThreads() {
		return frontier.runningDnsThreads.get();
	}

	@ManagedAttribute @Description("Number of working DNS threads")
	public int getWorkingDnsThreads() {
		return frontier.workingDnsThreads.get();
	}

	@ManagedAttribute
	public void setParsingThreads(final int parsingThreads) throws IllegalArgumentException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, IOException, NoSuchAlgorithmException {
		rc.parsingThreads = frontier.parsingThreads(parsingThreads);
	}

	@ManagedAttribute @Description("Number of parsing threads (usually, no more than the number of available cores)")
	public int getParsingThreads() {
		return rc.parsingThreads;
	}

	@ManagedAttribute
	public void setFetchFilter(String spec) throws ParseException {
		rc.fetchFilter = new FilterParser<>(URI.class).parse(spec);
	}

	@ManagedAttribute @Description("Filters that will be applied to all URLs out of the frontier to decide whether to fetch them")
	public String getFetchFilter() {
		return rc.fetchFilter.toString();
	}

	@ManagedAttribute
	public void setScheduleFilter(String spec) throws ParseException {
		rc.scheduleFilter = new FilterParser<>(Link.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all URLs obtained by parsing a page before scheduling them")
	public String getScheduleFilter() {
		return rc.scheduleFilter.toString();
	}

	@ManagedAttribute
	public void setParseFilter(String spec) throws ParseException {
		rc.parseFilter = new FilterParser<>(URIResponse.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to parse them")
	public String getParseFilter() {
		return rc.parseFilter.toString();
	}

	@ManagedAttribute
	public void setFollowFilter(String spec) throws ParseException {
		rc.followFilter = new FilterParser<>(FetchData.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to follow their links")
	public String getFollowFilter() {
		return rc.followFilter.toString();
	}

	@ManagedAttribute
	public void setStoreFilter(String spec) throws ParseException {
		rc.storeFilter = new FilterParser<>(FetchData.class).parse(spec);
	}

	@ManagedAttribute @Description("Filter that will be applied to all fetched responses to decide whether to store them")
	public String getStoreFilter() {
		return rc.storeFilter.toString();
	}

	@ManagedAttribute
	public void setResponseBodyMaxByteSize(final int responseBodyMaxByteSize) {
		rc.responseBodyMaxByteSize = responseBodyMaxByteSize;
	}

	@ManagedAttribute @Description("The maximum size (in bytes) of a response body (the exceeding part will not be stored)")
	public long getResponseBodyMaxByteSize () {
		return rc.responseBodyMaxByteSize;
	}

	@ManagedAttribute
	public void setKeepAliveTime(final int keepAliveTime) {
		rc.keepAliveTime = keepAliveTime;
		frontier.setRequests();
	}

	@ManagedAttribute @Description("If zero, connections are closed at each downloaded resource. Otherwise, the time span to download continuously from the same site using the same connection")
	public long getKeepAliveTime () {
		return rc.keepAliveTime;
	}

	@ManagedAttribute
	public void setSchemeAuthorityDelay(final long schemeAuthorityDelay) {
		rc.schemeAuthorityDelay = schemeAuthorityDelay;
	}

	@ManagedAttribute @Description("Delay in milliseconds between two consecutive fetches from the same scheme+authority")
	public long getSchemeAuthorityDelay() {
		return rc.schemeAuthorityDelay;
	}

	@ManagedAttribute
	public void setIpDelay(final long ipDelay) {
		if (ipDelay > 250)
			rc.ipDelay = ipDelay;
	}

	@ManagedAttribute @Description("Delay in milliseconds between two consecutive fetches from the same IP address")
	public long getIpDelay() {
		return rc.ipDelay;
	}

	@ManagedAttribute
	public void setCrawlRequestTTL(final long crawlRequestTTL) {
		rc.crawlRequestTTL = crawlRequestTTL;
	}

	@ManagedAttribute @Description("Time-to-live of a crawl request")
	public long getCrawlRequestTTL() {
		return rc.crawlRequestTTL;
	}

	@ManagedAttribute
	public void setMaxInstantSchemeAuthorityPerIP(final int maxInstantSchemeAuthorityPerIP) {
		rc.maxInstantSchemeAuthorityPerIP = maxInstantSchemeAuthorityPerIP;
	}

	@ManagedAttribute @Description("Maximum number of SchemeAuthority in a live WorkbenchEntry")
	public int getMaxInstantSchemeAuthorityPerIP() {
		return rc.maxInstantSchemeAuthorityPerIP;
	}

	@ManagedAttribute
	public void setMaxVisitStates(final int maxVisitStates) {
		rc.maxVisitStates = maxVisitStates;
	}

	@ManagedAttribute @Description("Maximum number of VisitStates in memory")
	public int getMaxVisitStates() {
		return rc.maxVisitStates;
	}

	@ManagedAttribute
	public void setCookieMaxByteSize(final int cookieMaxByteSize) {
		rc.cookieMaxByteSize = cookieMaxByteSize;
	}

	@ManagedAttribute @Description("Maximum size of cookie file")
	public int getCookieMaxByteSize() {
		return rc.cookieMaxByteSize;
	}

	@ManagedAttribute
	public void setSocketTimeout(final int socketTimeout) {
		rc.socketTimeout = socketTimeout;
		frontier.setRequests();
	}

	@ManagedAttribute @Description("Timeout in milliseconds for opening a socket")
	public int getSocketTimeout() {
		return rc.socketTimeout;
	}

	@ManagedAttribute
	public void setConnectionTimeout(final int connectionTimeout) {
		rc.connectionTimeout = connectionTimeout;
		frontier.setRequests();
	}

	@ManagedAttribute @Description("Socket connection timeout in milliseconds")
	public int getConnectionTimeout() {
		return rc.connectionTimeout;
	}

	@ManagedAttribute
	public void setDnsTimeout(final int dnsTimeout) {
		rc.dnsTimeout = dnsTimeout;
		Lookup.getDefaultResolver().setTimeout(Duration.ofMillis(rc.dnsTimeout));
	}

	@ManagedAttribute @Description("DNS query timeout in milliseconds")
	public int getDnsTimeout() {
		return rc.dnsTimeout;
	}

	@ManagedAttribute
	public void setMaximumTimeToFirstByte(final int maximumTimeToFirstByte) {
		rc.maximumTimeToFirstByte = maximumTimeToFirstByte;
	}

	@ManagedAttribute @Description("Maximum time allowed to first byte")
	public int getMaximumTimeToFirstByte() {
		return rc.maximumTimeToFirstByte;
	}

	@ManagedAttribute
	public void setMaximumFetchDuration(final int maximumFetchDuration) {
		rc.maximumFetchDuration = maximumFetchDuration;
	}

	@ManagedAttribute @Description("Maximum time allowed for fetching a page")
	public int getMaximumFetchDuration() {
		return rc.maximumFetchDuration;
	}
	@ManagedAttribute
	public void setMinimumDownloadSpeed(final int minimumDownloadSpeed) {
		rc.minimumDownloadSpeed = minimumDownloadSpeed;
	}

	@ManagedAttribute @Description("Minimum allowable download speed")
	public int getMinimumDownloadSpeed() {
		return rc.minimumDownloadSpeed;
	}

	public int minimumDownloadSpeed;

	@ManagedAttribute
	public void setRobotsExpiration(final long robotsExpiration) {
		rc.robotsExpiration = robotsExpiration;
	}

	@ManagedAttribute @Description("Milliseconds after which the robots.txt file is no longer considered valid")
	public long getRobotsExpiration() {
		return rc.robotsExpiration;
	}

	@ManagedAttribute
	public void setWorkbenchMaxByteSize(final long workbenchSize) {
		rc.workbenchMaxByteSize = workbenchSize;
	}

	@ManagedAttribute @Description("Maximum size of the workbench in bytes")
	public long getWorkbenchMaxByteSize() {
		return rc.workbenchMaxByteSize;
	}

	/*Statistical Properties, as reported by StatsThread */

	/*@ManagedAttribute @Description("The time elapsed since the start of the crawl")
	public long getTime() {
		return statsThread.requestLogger.millis();
	}*/

	@ManagedAttribute @Description("Approximate size of the workbench in bytes")
	public long getWorkbenchByteSize() {
		return frontier.weightOfpathQueriesInQueues.get();
	}

	@ManagedAttribute @Description("Size of the workbench entry set")
	public int getWorkbenchEntriesCount() {	return frontier.workbench.numberOfWorkbenchEntries(); }


	@ManagedAttribute @Description("Overall size of the store (includes archetypes and duplicates)")
	public long getStoreSize() {
		return frontier.archetypes() + frontier.duplicates.get();
	}

	@ManagedAttribute @Description("Number of stored archetypes")
	public long getArchetypes() {
		return frontier.archetypes();
	}

	@ManagedAttribute @Description("Number of stored archetypes having other status")
	public long getArchetypesOther() {
		return frontier.archetypesStatus[0].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 1xx")
	public long getArchetypes1xx() {
		return frontier.archetypesStatus[1].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 2xx")
	public long getArchetypes2xx() {
		return frontier.archetypesStatus[2].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 3xx")
	public long getArchetypes3xx() {
		return frontier.archetypesStatus[3].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 4xx")
	public long getArchetypes4xx() {
		return frontier.archetypesStatus[4].get();
	}

	@ManagedAttribute @Description("Number of stored archetypes having status 5xx")
	public long getArchetypes5xx() {
		return frontier.archetypesStatus[5].get();
	}

	@ManagedAttribute @Description("Statistics about the number of outlinks of each archetype")
	public String getArchetypeOutdegree(){
		return frontier.outdegree.toString();
	}

	@ManagedAttribute @Description("Statistics about the number of outlinks of each archetype, without considering the links to the same corresponding host")
	public String getArchetypeExternalOutdegree(){
		return frontier.externalOutdegree.toString();
	}

	@ManagedAttribute @Description("Statistic about the content length of each archetype")
	public String getArchetypeContentLength(){
		return frontier.contentLength.toString();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with text (case insensitive)")
	public long getArchetypeContentTypeText(){
		return frontier.contentTypeText.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with image (case insensitive)")
	public long getArchetypeContentTypeImage(){
		return frontier.contentTypeImage.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type starts with application (case insensitive)")
	public long getArchetypeContentTypeApplication(){
		return frontier.contentTypeApplication.get();
	}

	@ManagedAttribute @Description("Number of archetypes whose indicated content type does not start with text, image, or application (case insensitive)")
	public long getArchetypeContentTypeOthers(){
		return frontier.contentTypeOthers.get();
	}

	@ManagedAttribute @Description("Number of requests")
	public long getRequests() {
		return frontier.fetchedResources.get() + frontier.fetchedRobots.get();
	}

	@ManagedAttribute @Description("Number of responses")
	public long getResources() {
		return frontier.fetchedResources.get();
	}

	@ManagedAttribute @Description("Number of transferred bytes")
	public long getBytes() {
		return frontier.transferredBytes.get();
	}

	@ManagedAttribute @Description("Number of URLs received from other agents")
	public long getReceivedURLs() {
		return frontier.numberOfReceivedURLs.get();
	}

  @ManagedAttribute @Description("Number of fetched URLs sent to the frontier")
  public long getSentURLs() {
    return frontier.numberOfSentURLs.get();
  }

  @ManagedAttribute @Description("Number of duplicates")
	public long getDuplicates() {
		return frontier.duplicates.get();
	}

	@ManagedAttribute @Description("Percentage of duplicates")
	public double getDuplicatePercentage() {
		return 100.0 * frontier.duplicates.get() / (1 + frontier.archetypes());
	}

	@ManagedAttribute @Description("Number of ready URLs")
	public long getReadyURLs() {
		return frontier.receivedCrawlRequests.size();
	}

	@ManagedAttribute @Description("Number of FetchingThread waits")
	public long getFetchingThreadWaits() {
		return frontier.fetchingThreadWaits.get();
	}

	@ManagedAttribute @Description("Overall FetchingThread waiting time")
	public long getFetchingThreadTotalWaitTime() {
		return frontier.fetchingThreadWaitingTimeSum.get();
	}

	@ManagedAttribute @Description("URLs in VisitState queues")
	public long getURLsInQueues() {
		return frontier.pathQueriesInQueues.get();
	}

	@ManagedAttribute @Description("URLs in VisitState queues")
	public long getURLsInDiskQueues() {
		return frontier.pathQueriesInDiskQueues.get();
	}

	@ManagedAttribute @Description("Percentage of workbench maximum size in used")
	public double getURLsInQueuesPercentage() {
		return 100.0 * frontier.weightOfpathQueriesInQueues.get() / frontier.rc.workbenchMaxByteSize;
	}

	@ManagedAttribute @Description("Distribution of URL among all VisitState instances (in position i, number of instances having 2^i URLs)")
	public int[] getQueueDistribution() {
		return frontier.getStatsThread().dist;
	}

	@ManagedAttribute @Description("Number of unresolved VisitState instances")
	public long getUnresolved() {
		return frontier.getStatsThread().unresolved;
	}

	@ManagedAttribute @Description("Number of path+queries in broken VisitState instances")
	public long getBroken() {
		return frontier.getStatsThread().brokenPathQueryCount;
	}

	@ManagedAttribute @Description("Average number of VisitState instances in a WorkbenchEntry")
	public double getEntryAverage() {
		return frontier.getStatsThread().entrySummaryStats.mean();
	}

	@ManagedAttribute @Description("Maximum number of VisitState instances in a WorkbenchEntry")
	public double getEntryMax() {
		return frontier.getStatsThread().entrySummaryStats.max();
	}

	@ManagedAttribute @Description("Minimum number of VisitState instances in a WorkbenchEntry")
	public double getEntryMin() {
		return frontier.getStatsThread().entrySummaryStats.min();
	}

	@ManagedAttribute @Description("Variance of the number of VisitState instances in a WorkbenchEntry")
	public double getEntryVariance() {
		return frontier.getStatsThread().entrySummaryStats.variance();
	}

	@ManagedAttribute @Description("Number of VisitState instances")
	public int getVisitStates() {
		return frontier.getStatsThread().getVisitStates();
	}


	@ManagedAttribute @Description("Number of received VisitState instances")
	public long getReceivedVisitStates() {return frontier.receivedVisitStates.get(); }

	@ManagedAttribute @Description("Number of resolved VisitState instances")
	public long getResolvedVisitStates() {return frontier.resolvedVisitStates.get(); }

	@ManagedAttribute @Description("Number of entries on the workbench")
	public long getIPOnWorkbench() {
		return frontier.workbench.approximatedSize();
	}

	@ManagedAttribute @Description("Number of VisitState instances on the workbench")
	public long getVisitStatesOnWorkbench() {
		return (long)frontier.getStatsThread().entrySummaryStats.sum();
	}

	@ManagedAttribute @Description("Number of VisitState instances on the todo list")
	public long getToDoSize() {
		return frontier.todo.size();
	}

	@ManagedAttribute @Description("Number of VisitState instances on the refill list")
	public long getRefillSize() {
		return frontier.refill.size();
	}

	@ManagedAttribute @Description("Number of FetchingThread instances downloading data")
	public int getActiveFetchingThreads() {
		return  (int)(frontier.rc.fetchingThreads - frontier.results.size());
	}


	@ManagedAttribute @Description("Time spent in fetches")
	public long getFetchingDurationTotalMs() {
		return (frontier.fetchingDurationTotal.get());
	}

	@ManagedAttribute @Description("Number of fetches done so far")
	public long getFetchingCount() { return  frontier.fetchingCount.get();	}


	@ManagedAttribute @Description("Number of failed fetches done so far")
	public long getFetchingFailedCount() { return  frontier.fetchingFailedCount.get();	}


	@ManagedAttribute @Description("Number of failed fetches (Unkown host) done so far")
	public long getFetchingFailedHostCount() { return  frontier.fetchingFailedHostCount.get();	}

	@ManagedAttribute @Description("Number of failed fetches (Robots.txt denied) done so far")
	public long getFetchingFailedRobotsCount() { return  frontier.fetchingFailedRobotsCount.get();	}

	@ManagedAttribute @Description("Number of robots fetches done so far")
	public long getFetchingRobotsCount() { return  frontier.fetchingRobotsCount.get();	}

	@ManagedAttribute @Description("Number of fetches finished with a timeout")
	public long getFetchingTimeoutCount() { return  frontier.fetchingTimeoutCount.get(); }

	@ManagedAttribute @Description("Time spent in parsing")
	public long getParsingDurationTotal() { return  frontier.parsingDurationTotal.get(); }

	@ManagedAttribute @Description("Number of parsing done")
	public long getParsingCount() { return  frontier.parsingCount.get(); }

	@ManagedAttribute @Description("Number of parsing robots")
	public long getParsingRobotsCount() { return  frontier.parsingRobotsCount.get(); }

	@ManagedAttribute @Description("Number of parsing failed")
	public long getParsingErrorCount() { return  frontier.parsingErrorCount.get(); }

	@ManagedAttribute @Description("Number of parsing failed with an exception")
	public long getParsingExceptionCount() { return  frontier.parsingExceptionCount.get(); }

	@ManagedAttribute @Description("Number of FetchingThread instances waiting for parsing")
	public int getReadyToParse() {
		return  (int)frontier.results.size();
	}

	@ManagedAttribute @Description("Number of unknown hosts")
	public int getUnknownHosts() {
		return  frontier.unknownHosts.size();
	}

	@ManagedAttribute @Description("Number of broken VisitState instances")
	public long getBrokenVisitStates() {
		return  frontier.brokenVisitStates.get();
	}

	@ManagedAttribute @Description("Number of broken VisitState instances on the workbench")
	public long getBrokenVisitStatesOnWorkbench() {
		return  frontier.getStatsThread().brokenVisitStatesOnWorkbench;
	}

	@ManagedAttribute @Description("Number of new VisitState instances waiting to be resolved")
	public int getWaitingVisitStates() {
		return frontier.newVisitStates.size();
	}

	@ManagedAttribute @Description("Number of VisitState instances with path+queries on disk")
	public long getVisitStatesOnDisk() {
		return frontier.getStatsThread().getVisitStatesOnDisk();
	}

	@ManagedAttribute @Description("Current required front size")
	public long getRequiredFrontSize() {
		return frontier.requiredFrontSize.get();
	}

	@ManagedAttribute
	public void setRequiredFrontSize(long requiredFrontSize) {
		frontier.requiredFrontSize.set(requiredFrontSize);
	}

	@ManagedAttribute @Description("Current required front size")
	public long getFrontSize() {
		return frontier.getCurrentFrontSize();
	}

	/**
	 * Hack for allowing hostnames with "_"
	 */
	private static void patchUriField(String methodName, String fieldName)
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
		Method lowMask = URI.class.getDeclaredMethod(methodName, String.class);
		lowMask.setAccessible(true);
		long lowMaskValue = (long) lowMask.invoke(null, "-_");

		Field lowDash = URI.class.getDeclaredField(fieldName);

		Field modifiers = Field.class.getDeclaredField("modifiers");
		modifiers.setAccessible(true);
		modifiers.setInt(lowDash, lowDash.getModifiers() & ~Modifier.FINAL);

		lowDash.setAccessible(true);
		lowDash.setLong(null, lowMaskValue);
	}

	public static void main(final String arg[]) throws Exception {
		//patchUriField("lowMask", "L_DASH");
		//patchUriField("highMask", "H_DASH");

		final SimpleJSAP jsap = new SimpleJSAP(Agent.class.getName(), "Starts a BUbiNG agent (note that you must enable JMX by means of the standard Java system properties).",
				new Parameter[] {
					new FlaggedOption("jmxHost", JSAP.STRING_PARSER, InetAddress.getLocalHost().getHostAddress(), JSAP.REQUIRED, 'h', "jmx-host", "The IP address (possibly specified by a host name) that will be used to expose the JMX RMI connector to other agents."),
					new FlaggedOption("rootDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "root-dir", "The root directory."),
					new Switch("new", 'n', "new", "Start a new crawl"),
					new Switch("priority", 'p', "priority", "Priority crawler"),
					new FlaggedOption("properties", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'P', "properties", "The properties used to configure the agent."),
					new FlaggedOption("id", JSAP.INTEGER_PARSER, "0", JSAP.REQUIRED, 'i', "id", "The agent id."),
					new UnflaggedOption("name", JSAP.STRING_PARSER, JSAP.REQUIRED, "The agent name (an identifier that must be unique across the group).")
			});

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		// Safety : long running process with ssl connection get OOM because SSL cache is unbounded, this ensure that the cache is bounded
		if (System.getProperty("javax.net.ssl.sessionCacheSize") == null)
			System.setProperty("javax.net.ssl.sessionCacheSize","8192");

		// JMX *must* be set up.
		final String portProperty = System.getProperty(JMX_REMOTE_PORT_SYSTEM_PROPERTY);
		if (portProperty == null) throw new IllegalArgumentException("You must specify a JMX service port using the property " + JMX_REMOTE_PORT_SYSTEM_PROPERTY);

		final String name = jsapResult.getString("name");
		final int id = jsapResult.getInt("id");
		final String host = jsapResult.getString("jmxHost");
		final int port = Integer.parseInt(portProperty);

		final BaseConfiguration additional = new BaseConfiguration();
		additional.addProperty("name", name);
		additional.addProperty("pulsarFrontierNodeId", Integer.toString(id));
		additional.addProperty("crawlIsNew", jsapResult.getBoolean("new"));
		additional.addProperty("priorityCrawl", jsapResult.getBoolean("priority"));
		if (jsapResult.userSpecified("rootDir")) additional.addProperty("rootDir", jsapResult.getString("rootDir"));

		final RuntimeConfiguration rc = new RuntimeConfiguration( new StartupConfiguration(jsapResult.getString("properties"), additional) );
		final Agent agent = new Agent(host, port, rc);

		final Thread shutdownHookThread = new Thread( () -> {
			try {
				LOGGER.warn( "ShutdownHook : invoked" );
				agent.terminate();
			}
			catch (Exception e) {
				LOGGER.error("Error during shutdown",e);
			}
			LOGGER.warn( "ShutdownHook : exited" );
		} );

		Runtime.getRuntime().addShutdownHook( shutdownHookThread );
		LOGGER.warn( "Invoking agent.run()" );
		agent.run();
		LOGGER.warn( "agent.run() exited" );
		Runtime.getRuntime().removeShutdownHook( shutdownHookThread );
	}

}
