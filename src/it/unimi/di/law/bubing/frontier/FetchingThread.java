package it.unimi.di.law.bubing.frontier;

import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.common.UnexpectedException;
import com.exensa.wdl.protobuf.ProtoHelper;
import com.exensa.wdl.protobuf.crawler.EnumFetchStatus;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.FetchData;
import it.unimi.di.law.bubing.util.URLRespectsRobots;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

/**
 * A thread fetching pages that will be then analyzed by a {@link ParsingThread}.
 * <p>
 * <p>Instances of this class iteratively extract from {@link Frontier#todo} (using
 * polling and exponential backoff) a ready {@link VisitState} that has been
 * previously enqueued by the {@link TodoThread} and use their embedded
 * {@link FetchData} to fetch the first URL (or possibly the first few URLs, depending on {@link RuntimeConfiguration#keepAliveTime})
 * from the {@link VisitState} queue. Once the fetch is over, the embedded
 * {@link FetchData} is enqueued in {@link Frontier#results} and the thread
 * waits for a signal on the {@link FetchData}. The {@link ParsingThread} that
 * retrieves the {@link FetchData} must signal back that the {@link FetchData}
 * instances can be reused when it has finished to use its contents.
 * <p>
 * <p>The design of the interaction between instances of this class, the
 * {@link TodoThread} and instances of {@link ParsingThread} minimizes
 * contention by sandwiching all {@link FetchingThread} instances between two
 * wait-free queues (signalling back that a {@link FetchData} can be reused
 * causes of course no contention). Exponential backoff should happen rarely in
 * a full-speed crawl, as the {@linkplain Frontier#todo todo} queue is almost
 * always nonempty.
 * <p>
 * <p>Instances of this class do not access any shared data structure, except for
 * logging. It is expected that large instances of BUbiNG use thousands of
 * fetching threads to download from a large number of sites in parallel.
 * <p>
 * <p>This class implements {@link Closeable}: the {@link #close()} methods simply closes the underlying {@link FetchData} instance.
 */
public final class FetchingThread extends Thread implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FetchingThread.class);


  /**
   * Whether we should stop (used also to reduce the number of threads).
   */
  public volatile boolean stop;
  /**
   * A reference to the frontier.
   */
  private final Frontier frontier;
  /**
   * The synchronous HTTP client used by this thread.
   */
  private final HttpClient httpClient;
  /**
   * The fetched HTTP response used by this thread.
   */
  private FetchData fetchData;
  /**
   * The cookie store used by {@link #httpClient}.
   */
  private final BasicCookieStore cookieStore;

  /**
   * An SSL context that accepts all self-signed certificates.
   */
  private static final SSLContext TRUST_SELF_SIGNED_SSL_CONTEXT;

  static {
    try {
      TRUST_SELF_SIGNED_SSL_CONTEXT = SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build();
    } catch (Exception cantHappen) {
      throw new RuntimeException(cantHappen.getMessage(), cantHappen);
    }
  }

  /**
   * An SSL context that accepts all certificates
   */
  private static final SSLContext TRUST_ALL_CERTIFICATES_SSL_CONTEXT;

  static {
    try {
      TRUST_ALL_CERTIFICATES_SSL_CONTEXT = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
        public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
          return true;
        }
      }).build();
    } catch (Exception cantHappen) {
      throw new RuntimeException(cantHappen.getMessage(), cantHappen);
    }
  }

  /**
   * A support class that makes it possible to plug in a custom DNS resolver.
   */
  protected static final class BasicHttpClientConnectionManagerWithAlternateDNS
      extends BasicHttpClientConnectionManager {

    static Registry<ConnectionSocketFactory> getDefaultRegistry() {
      // setup a Trust Strategy that allows all certificates.
      //
      SSLContext sslContext = TRUST_ALL_CERTIFICATES_SSL_CONTEXT;
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory())
          .register("https",
              new SSLConnectionSocketFactory(sslContext,
                  new String[]{
                      "TLSv1",
                      "SSLv3",
                      "TLSv1.1",
                      "TLSv1.2",
                      "TLSv1.3"
                  }, null, new NoopHostnameVerifier()))
          .build();
    }

    public BasicHttpClientConnectionManagerWithAlternateDNS(final DnsResolver dnsResolver) {
      super(getDefaultRegistry(), null, null, dnsResolver);
    }
  }

  private static int length(final String s) {
    return s == null ? 0 : s.length();
  }

  /**
   * Returns the list of cookies in a given store in the form of an array, limiting their overall size
   * (only the maximal prefix of cookies satisfying the size limit is returned). Note that the size limit is expressed
   * in bytes, but the actual memory footprint is larger because of object overheads and because strings
   * are made of 16-bits characters. Moreover, cookie attributes are not measured when evaluating size.
   *
   * @param url               the URL which generated the cookies (for logging purposes).
   * @param cookieStore       a cookie store, usually generated by a response.
   * @param cookieMaxByteSize the maximum overall size of cookies in bytes.
   * @return the list of cookies in a given store in the form of an array, with limited overall size.
   */
  public static Cookie[] getCookies(final URI url, final CookieStore cookieStore, final int cookieMaxByteSize) {
    int overallLength = 0, i = 0;
    final List<Cookie> cookies = cookieStore.getCookies();
    for (Cookie cookie : cookies) {
      /* Just an approximation, and doesn't take attributes into account, but
       * there is no way to enumerate the attributes of a cookie. */
      overallLength += length(cookie.getName());
      overallLength += length(cookie.getValue());
      overallLength += length(cookie.getDomain());
      overallLength += length(cookie.getPath());
      if (overallLength > cookieMaxByteSize) {
        LOGGER.warn("URL " + url + " returned too large cookies (" + overallLength + " characters)");
        return cookies.subList(0, i).toArray(new Cookie[i]);
      }
      i++;
    }
    return cookies.toArray(new Cookie[cookies.size()]);
  }

  public static void storeCookies(final VisitState visitState, final CookieStore cookieStore ) {
    if ( visitState.cookies != null ) {
      for ( final Cookie cookie : visitState.cookies )
        cookieStore.addCookie( cookie );
    }
  }

  /**
   * Creates a new fetching thread.
   *
   * @param frontier a reference to the {@link Frontier}.
   * @param index    the index of this thread (only for logging purposes).
   */
  public FetchingThread(final Frontier frontier, final int index) throws NoSuchAlgorithmException, IllegalArgumentException, IOException {
    setName(this.getClass().getSimpleName() + '-' + index);
    setPriority(Thread.MIN_PRIORITY); // Low priority; there will be thousands of this guys around.
    this.frontier = frontier;

    final BasicHttpClientConnectionManager connManager = new BasicHttpClientConnectionManagerWithAlternateDNS(frontier.rc.dnsResolver);

    connManager.closeIdleConnections(0, TimeUnit.MILLISECONDS);
    connManager.setConnectionConfig(ConnectionConfig.custom().setBufferSize(8 * 1024).build()); // TODO: make this configurable

    cookieStore = new BasicCookieStore();

    BasicHeader[] headers = {
        new BasicHeader("From", frontier.rc.userAgentFrom),
        new BasicHeader("Accept", "text/html;q=0.95,text/*;q=0.9,*/*;q=0.8"),
        new BasicHeader("Accept-Language", "*"),
        new BasicHeader("Accept-Charset", "*")
    };

    httpClient = HttpClients.custom()
        .setSSLHostnameVerifier(new NoopHostnameVerifier()) // why would we need to do it twice ?
        .setSSLContext(frontier.rc.acceptAllCertificates ? TRUST_ALL_CERTIFICATES_SSL_CONTEXT : TRUST_SELF_SIGNED_SSL_CONTEXT)
        .setConnectionManager(connManager)
        .setConnectionReuseStrategy(frontier.rc.keepAliveTime == 0 ? NoConnectionReuseStrategy.INSTANCE : DefaultConnectionReuseStrategy.INSTANCE)
        .setUserAgent(frontier.rc.userAgent)
        .setDefaultCookieStore(cookieStore)
        .setDefaultHeaders(ObjectArrayList.wrap(headers))
        .build();
    fetchData = new FetchData(frontier.rc);
    frontier.fetchDataCount.incrementAndGet();
  }

  @Override
  public void run() {
    try {
      LOGGER.warn( "thread [started]" );
      frontier.runningFetchingThreads.incrementAndGet();
      while ( !stop ) {
        final VisitState visitState = getNextVisitState();
        if ( visitState != null ) {
          frontier.workingFetchingThreads.incrementAndGet(); // decrement is done below or in processFetchData()
          if (!processVisitState(visitState) ) {
            frontier.done.add(visitState);
            frontier.workingFetchingThreads.decrementAndGet();
          } else
            processFetchData();
        }
      }
    }
    catch ( InterruptedException e ) {
      LOGGER.error( "Interrupted", e );
    }
    catch ( Throwable e ) {
      LOGGER.error( "Unexpected exception", e );
    }
    finally {
      LOGGER.warn( "thread [stopping]" );
      LOGGER.warn( "thread [stopped]" );
      frontier.runningFetchingThreads.decrementAndGet();
    }
  }

  public void close() throws IOException {
    FetchData fd = fetchData;
    if (fd != null)
      fd.close();
  }

  /**
   * Causes the {@link FetchData} used by this thread to be {@linkplain FetchData#abort()} (whence, the corresponding connection to be closed).
   */
  public void abort() {
    FetchData fd = fetchData;
    if (fd != null)
      fd.abort();
  }

  private VisitState getNextVisitState() throws InterruptedException {
    long waitTime = 0;

    frontier.rc.ensureNotPaused();
    VisitState visitState;
    for ( int i=0; (visitState=frontier.todo.poll()) == null; ++i ) {
      if (stop) return null;
      long newSleep = 1 << Math.min(i, 10);
      waitTime += newSleep;
      Thread.sleep( newSleep );
      frontier.rc.ensureNotPaused();
    }

    if (waitTime > 0) {
      frontier.updateRequestedFrontSize();
      frontier.updateFetchingThreadsWaitingStats(waitTime);
    }

    if (LOGGER.isTraceEnabled()) LOGGER.trace("Acquired visit state {}", visitState);
    return visitState;
  }

  private boolean processVisitState( final VisitState visitState ) throws InterruptedException {
    if (LOGGER.isTraceEnabled()) LOGGER.trace("Processing VisitState for {}", PulsarHelper.toString(PulsarHelper.schemeAuthority(visitState.schemeAuthority).build()));
    final MsgURL.Key schemeAuthorityProto = PulsarHelper.schemeAuthority(visitState.schemeAuthority).build();

    while ( !stop && !visitState.isEmpty() ) {
      frontier.rc.ensureNotPaused();

      if ( fetchData == null )
        fetchData = getAvailableFetchData(); // block until success or stop requested
      if ( fetchData == null )
        continue; // stop requested

      final byte[] minimalCrawlRequestSerialized = visitState.dequeue(); // contains a zPathQuery
      final boolean isRobots = minimalCrawlRequestSerialized == VisitState.ROBOTS_PATH;
      try {
        final MsgFrontier.CrawlRequest.Builder crawlRequest = FetchInfoHelper.createCrawlRequest(schemeAuthorityProto, minimalCrawlRequestSerialized);
        // First check that the crawlRequest is still valid
        if (ProtoHelper.ttlHasExpired(crawlRequest.getCrawlInfo().getScheduleTimeMinutes(), frontier.rc.crawlRequestTTL)) {
          if (LOGGER.isTraceEnabled()) {
            final URI url = BURL.fromNormalizedSchemeAuthorityAndPathQuery(visitState.schemeAuthority, HuffmanModel.defaultModel.decompress(crawlRequest.getUrlKey().getZPathQuery().toByteArray()));
            LOGGER.trace("CrawlRequest for {} has expired", url.toString());
          }
          continue;
        }

        final URI url = BURL.fromNormalizedSchemeAuthorityAndPathQuery(visitState.schemeAuthority, HuffmanModel.defaultModel.decompress(crawlRequest.getUrlKey().getZPathQuery().toByteArray()));

        if (LOGGER.isTraceEnabled()) LOGGER.trace("Next URL to fetch : {}", url);

        if (BlackListing.checkBlacklistedHost(frontier, url)) {
          if (!isRobots) // FIXME: don't send fetchInfo for robots.txt
            frontier.enqueue(FetchInfoHelper.fetchInfoFailedBlackList(crawlRequest, visitState));
          frontier.fetchingFailedCount.incrementAndGet();
          continue; // next PathQuery
        }

        if (BlackListing.checkBlacklistedIP(frontier, url, visitState.workbenchEntry.ipAddress)) {
          if (!isRobots) // FIXME: don't send fetchInfo for robots.txt
            frontier.enqueue(FetchInfoHelper.fetchInfoFailedBlackList(crawlRequest, visitState));
          frontier.fetchingFailedCount.incrementAndGet();
          continue; // next PathQuery
        }

        if (isRobots) {
          if (tryFetch(visitState, crawlRequest, url, true)) // FIXME: may return true even if fetch has failed...
            return true; // process fetch data
          break; // skip to next SchemeAuthority
        }

        if (!frontier.rc.fetchFilter.apply(url)) {
          if (LOGGER.isDebugEnabled()) LOGGER.debug("URL {} filtered out", url);
          frontier.enqueue(FetchInfoHelper.fetchInfoFailedFiltered(crawlRequest, visitState));
          frontier.fetchingFailedCount.incrementAndGet();
          continue; // skip to next PathQuery
        }

        if (visitState.robotsFilter != null && !visitState.robotsFilter.isAllowed(url.toString())) {
          if (LOGGER.isDebugEnabled()) LOGGER.debug("URL {} disallowed by robots filter", url);
          frontier.enqueue(FetchInfoHelper.fetchInfoFailedRobots(crawlRequest, visitState));
          frontier.fetchingFailedRobotsCount.incrementAndGet();
          frontier.fetchingFailedCount.incrementAndGet();
          continue; // skip to next PathQuery
        }

        if (RuntimeConfiguration.FETCH_ROBOTS && visitState.robotsFilter == null)
          LOGGER.warn("Null robots filter for " + it.unimi.di.law.bubing.util.Util.toString(visitState.schemeAuthority));

        if (tryFetch(visitState, crawlRequest, url, false)) // FIXME: may return true even if fetch has failed...
          return true; // process fetch data
        break; // skip to next SchemeAuthority
      } catch (InvalidProtocolBufferException ipbe) {
        throw new UnexpectedException(ipbe);
      }
    }
    return false; // skip to next SchemeAuthority
  }


  private void processFetchData() {
    if ( checkAndUpdateFetchData() ) {
      frontier.workingFetchingThreads.decrementAndGet();
      frontier.results.add( fetchData );
      fetchData = null;
    }
    else {
      frontier.workingFetchingThreads.decrementAndGet();
      if ( !fetchData.robots ) // FIXME: don't send fetchInfo for robots.txt
        frontier.enqueue(fetchInfoFailed());
      frontier.done.add( fetchData.visitState );
    }
  }


  private MsgCrawler.FetchInfo fetchInfoFailed() {
    MsgCrawler.FetchInfo.Builder fetchInfoBuilder = MsgCrawler.FetchInfo.newBuilder();
    fetchInfoBuilder
      .setUrlKey(fetchData.getCrawlRequest().getUrlKey())
      .setFetchTimeToFirstByte((int)(fetchData.firstByteTime - fetchData.startTime))
      .setFetchDuration( (int)(fetchData.endTime - fetchData.startTime) )
      .setFetchDate( (int)(fetchData.startTime / (24*60*60*1000)) )
      .setFetchTimeMinutes( (int)(fetchData.startTime / ( 60*1000)) );

    if (ExceptionHelper.EXCEPTION_TO_FETCH_STATUS.containsKey(fetchData.exception.getClass()))
      fetchInfoBuilder.setFetchStatusValue(ExceptionHelper.EXCEPTION_TO_FETCH_STATUS.getInt(fetchData.exception.getClass()));
    else
      fetchInfoBuilder.setFetchStatusValue(EnumFetchStatus.Enum.UNKNOWN_FAILURE_VALUE);
    return fetchInfoBuilder.build();
  }

  private boolean checkAndUpdateFetchData() {
    final RuntimeConfiguration rc = frontier.rc;
    final VisitState visitState = fetchData.visitState;

    // This is always the same, independently of what will happen.
    final int entrySize = visitState.workbenchEntry.size();
    long ipDelay = rc.ipDelay;

    visitState.workbenchEntry.nextFetch = fetchData.endTime + (long)(ipDelay + visitState.workbenchEntry.delay);

    if ( !checkFetchDataException() )
      return false; // don't parse

    //final byte[] firstPath = visitState.dequeue();
    //if (LOGGER.isTraceEnabled())
    //	LOGGER.trace("Dequeuing " + it.unimi.di.law.bubing.util.Util.zToString(firstPath) + " after fetching " + fetchData.uri() + "; " + (visitState.isEmpty()
    //    ? "visit state is now empty "
    //    : "first path now is " + it.unimi.di.law.bubing.util.Util.zToString(visitState.firstPath())));
    visitState.nextFetch = fetchData.endTime + Math.max(frontier.rc.schemeAuthorityDelay, visitState.crawlDelayMS); // Regular delay

    if ( fetchData.robots ) {
      frontier.fetchedRobots.incrementAndGet();
      // FIXME: following is done by ParsingThread
      //frontier.robotsWarcParallelOutputStream.get().write(new HttpResponseWarcRecord(fetchData.uri(), fetchData.response()));
      //if ((visitState.robotsFilter = URLRespectsRobots.parseRobotsResponse(fetchData, rc.userAgent)) == null) {
      //  // We go on getting/creating a workbench entry only if we have robots permissions.
      //  visitState.schedulePurge();
      //  LOGGER.warn("Visit state " + visitState + " killed by null robots.txt");
      //}
      visitState.lastRobotsFetch = fetchData.endTime;
    }
    else {
      fetchData.visitState.nbFetched ++;
      // Do NOT Set cookies received in cookieStore in visitState (default is : no cookie, only if required by redirect)
      // fetchData.visitState.cookies = getCookies( fetchData.uri(), cookieStore, frontier.rc.cookieMaxByteSize );
      frontier.fetchedResources.incrementAndGet();
    }

    return true; // do parse
  }

  private boolean checkFetchDataException() {  // FIXME: see if this can be done by FetchingThread
    final VisitState visitState = fetchData.visitState;

    if ( fetchData.exception == null ) {
      if ( visitState.lastExceptionClass != null ) {
        frontier.brokenVisitStates.decrementAndGet();
        visitState.lastExceptionClass = null;
      }
      return true;
    }

    final Class<? extends Throwable> exceptionClass = fetchData.exception.getClass();

    if (LOGGER.isDebugEnabled()) LOGGER.debug("Exception while fetching " + fetchData.uri(), fetchData.exception);
    else if (LOGGER.isInfoEnabled()) LOGGER.info("Exception " + exceptionClass + "(" + fetchData.exception.getMessage() + ") while fetching " + fetchData.uri());

    if (visitState.lastExceptionClass == exceptionClass )
      visitState.retries += 1; // An old problem
    else { // A new problem
      // If the visit state *just broke down*, we increment the number of broken visit states.
      if (visitState.lastExceptionClass == null)
        frontier.brokenVisitStates.incrementAndGet();
      visitState.lastExceptionClass = exceptionClass;
      visitState.retries = 0;
    }

    if (visitState.retries < ExceptionHelper.EXCEPTION_TO_MAX_RETRIES.getInt(exceptionClass)) {
      final long delay = ExceptionHelper.EXCEPTION_TO_WAIT_TIME.getLong(exceptionClass) << visitState.retries;
      // Exponentially growing delay
      visitState.nextFetch = fetchData.endTime + delay;
      if (LOGGER.isInfoEnabled()) LOGGER.info("Will retry URL " + fetchData.uri() + " of visit state " + visitState + " for " + exceptionClass.getSimpleName() + " with delay " + delay);
    }
    else {
      frontier.brokenVisitStates.decrementAndGet();
      // Note that *any* repeated error on robots.txt leads to dropping the entire site => TODO : check if it's a good idea
      if (ExceptionHelper.EXCEPTION_HOST_KILLER.contains(exceptionClass) || fetchData.robots) {
        // Drain URLs in visitstate, creating adequate error fetch info
        try {
          FetchInfoHelper.drainVisitStateForError(frontier, visitState);
          visitState.schedulePurge();
          if (LOGGER.isInfoEnabled()) LOGGER.info("Visit state " + visitState + " killed by " + exceptionClass.getSimpleName() + " (URL: " + fetchData.uri() + ")");
        } catch (InterruptedException e) {
          LOGGER.error( "Interrupted", e );
        }
      }
      else {
        visitState.lastExceptionClass = null;
        // Regular delay
        visitState.nextFetch = fetchData.endTime + Math.max(frontier.rc.schemeAuthorityDelay, visitState.crawlDelayMS);
        if (LOGGER.isDebugEnabled()) LOGGER.debug("URL " + fetchData.uri() + " killed by " + exceptionClass.getSimpleName());
      }
    }

    return false;
  }

  private FetchData getAvailableFetchData() throws InterruptedException {
    frontier.rc.ensureNotPaused();
    FetchData fetchData;
    for ( int i = 0; ((fetchData = frontier.availableFetchData.poll()) == null)/* || fetchData.inUse*/; i++) {
      if (stop) return null;
      long newSleep = 1 << Math.min(i, 10);
      Thread.sleep(newSleep);
      frontier.rc.ensureNotPaused();
    }
    return fetchData;
  }

  /**
   * Check whether two URIs are equivalent. For instance http://example.com/p?PHPSESSID=SDFSFZ4352356 is equivalent to http://example.com/p
   * @param a first URI
   * @param b second URI
   * @return
   */
  private boolean _isEquivalentURI(URI a, URI b) {
    if ((a.getScheme() == null || b.getScheme() == null) && a.getScheme() != b.getScheme())
      return false;
    if (!a.getScheme().equals(b.getScheme()))
      return false;
    if ((a.getAuthority() == null || b.getAuthority() == null) && a.getAuthority() != b.getAuthority())
      return false;
    if (!a.getAuthority().equals(b.getAuthority()))
      return false;
    if (!a.getHost().equals(b.getHost()))
      return false;
    if ((a.getPath() == null || b.getPath() == null) && a.getPath() != b.getPath())
      return false;
      if (!a.getPath().equals(b.getPath()))
      return false;
    if (a.getQuery() == null || b.getQuery() == null)
      return a.getQuery() == b.getQuery();
    return (BURL.normalizeQuery(a.getQuery()).equals(BURL.normalizeQuery(b.getQuery())));
  }

  private boolean isEquivalentURI(URI a, URI b) {
    if((a.toString().equals(b.toString()))) {
      if (LOGGER.isTraceEnabled())
        LOGGER.trace("IsSame : {}, {}", a.toString(), b.toString());
      return true;
    }

    boolean isEquivalent = _isEquivalentURI(a,b);
    if (LOGGER.isTraceEnabled() && isEquivalent)
      LOGGER.trace("IsEquivalent : {}, {}", a.toString(), b.toString());
    return isEquivalent;
  }

  private boolean tryFetch( final VisitState visitState, final MsgFrontier.CrawlRequest.Builder crawlRequest, final URI url, final boolean robots ) {
    if (LOGGER.isTraceEnabled()) LOGGER.trace("Processing {}",url);

    cookieStore.clear();
    boolean finished = false;
    int attempt = 0; // number of self-redirect attempts
    while (!finished && attempt < 2) { // first attempt with redirects enabled. If terminal URI is != requestedUri, then retry without redirects
      attempt ++;
      if (robots)
        visitState.robotsFilter = URLRespectsRobots.EMPTY_ROBOTS_FILTER;
      else
        storeCookies(visitState, cookieStore);

      try {
        final RequestConfig requestConfig = robots
          ? frontier.robotsRequestConfig
          : ((attempt == 1) ? frontier.defaultRequestConfig : frontier.noRedirectRequestConfig);
        fetchData.fetch(url, crawlRequest.build(), httpClient, requestConfig, visitState, robots);
        // Deal with rate limiting situation
        if (fetchData.response() != null && fetchData.response().getStatusLine() != null && fetchData.response().getStatusLine().getStatusCode() == 429) {
          final long endTime = System.currentTimeMillis();
          visitState.workbenchEntry.nextFetch = endTime + 3 * frontier.rc.ipDelay + 3 * visitState.workbenchEntry.delay;
          visitState.nextFetch = endTime + 3 * + Math.max(frontier.rc.schemeAuthorityDelay, visitState.crawlDelayMS);
          if (robots)
            visitState.forciblyEnqueueRobotsFirst();
          else
            visitState.enqueueCrawlRequest(crawlRequest.build().toByteArray());

          LOGGER.info("Received HTTP code 429 for {}, waiting extratime, slow down and retrying later", url.toString());
          visitState.workbenchEntry.increaseDelay();
          return false;
        }
        // Special case redirected to another url
        if (attempt == 1 && fetchData.hasRedirects() &&  !isEquivalentURI(fetchData.getTerminalURI(),fetchData.uri())) {
          LOGGER.debug("Redirecting {} to ({}) : retrying without redirection to get first hop", url.toString(), fetchData.getTerminalURI());
          finished = false;
          continue; // retry with cookies
        }
        finished = true;
      } catch (Throwable shouldntHappen) {
        /* This shouldn't really happen--it's a bug that must be reported to the ASF team.
         * We cannot rely on the internal state of fetchData being OK, so we just discard it
         * and stop the keepalive download. We must perform some bookkeeping usually
         * performed by a ParsingThread. */
        LOGGER.error("Unexpected exception during fetch of " + url, shouldntHappen);
        final long endTime = System.currentTimeMillis();
        visitState.workbenchEntry.nextFetch = endTime + frontier.rc.ipDelay + visitState.workbenchEntry.delay;
        visitState.nextFetch = endTime + Math.max(frontier.rc.schemeAuthorityDelay, visitState.crawlDelayMS);
        return false;
      } finally {
        if (finished) {
          frontier.fetchingCount.incrementAndGet();
          if (robots)
            frontier.fetchingRobotsCount.incrementAndGet();
          frontier.fetchingDurationTotal.addAndGet(fetchData.endTime - fetchData.startTime);
        }
      }
    }

    if (fetchData.exception != null && (
        fetchData.exception instanceof java.net.SocketException
            || fetchData.exception instanceof java.net.SocketTimeoutException
            || fetchData.exception instanceof org.apache.http.conn.ConnectTimeoutException))
      frontier.fetchingTimeoutCount.incrementAndGet();

    if (LOGGER.isTraceEnabled()) LOGGER.trace("Fetched {} in {} ms", url.toString(), fetchData.endTime - fetchData.startTime);
    frontier.speedDist.incrementAndGet(Math.min(frontier.speedDist.length() - 1, Fast.mostSignificantBit(8 * fetchData.length() / (1 + fetchData.endTime - fetchData.startTime)) + 1));
    frontier.transferredBytes.addAndGet(fetchData.length());

    return true;
  }
}
