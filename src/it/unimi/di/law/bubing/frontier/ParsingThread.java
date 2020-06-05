package it.unimi.di.law.bubing.frontier;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import com.exensa.wdl.common.LanguageCodes;
import com.exensa.wdl.protobuf.link.MsgLink;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import it.unimi.di.law.bubing.categories.TextClassifier;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import it.unimi.di.law.bubing.parser.*;
import it.unimi.di.law.bubing.parser.html.RobotsTagState;
import it.unimi.di.law.bubing.util.*;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.util.InspectableCachedHttpEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;

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

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.store.Store;
import it.unimi.di.law.warc.filters.Filter;
//RELEASE-STATUS: DIST

/** A thread parsing pages retrieved by a {@link FetchingThread}.
 *
 * <p>Instances of this class iteratively extract from {@link Frontier#results} (using polling and exponential backoff)
 * a {@link FetchData} that has been previously enqueued by a {@link FetchingThread}.
 * The content of the response is analyzed and the body of the response is possibly parsed, and its
 * digest is computed.
 * Newly discovered (during parsing) URLs are
 * {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued to the frontier}.
 * Then, a signal is issued on the {@link FetchData}, so that the owner (a {@link FetchingThread}}
 * can work on a different URL or possibly {@link Workbench#release(VisitState) release the visit state}.
 *
 * <p>At each step (fetching, parsing, following the URLs of a page, scheduling new URLs, storing) a
 * configurable {@link Filter} selects whether a URL is eligible for a specific activity.
 * This makes it possible a very fine-grained configuration of the crawl. BUbiNG will also check
 * that it is respecting the <code>robots.txt</code> protocol, and <em>this behavior cannot be
 * disabled programmatically</em>. If you want to crawl the web without respecting the protocol,
 * you'll have to write your own BUbiNG variant (for which we will be not responsible).
 */
public class ParsingThread extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParsingThread.class);

  /** A small gadget used to insert links in the frontier. It should be {@linkplain #init initialized}
   *  specifying URI and scheme/authority of the page being visited and the robot filter to be
   *  applied. Then, one or more URLs can be
   *  {@linkplain Frontier#enqueue(MsgCrawler.FetchInfo) enqueued}: the actual
   *  enqueuing takes place only if the URL passes both the schedule and the robots filter.
   */
  private static final class FrontierEnqueuer
  {
    private static final boolean ASSERTS = false;
    private final Frontier frontier;
    private final TextClassifier classifier;
    private final Filter<Link> scheduleFilter;
    private byte[] schemeAuthority; // FIXME: TODO: not used anymore
    private URI source;
    private URLRespectsRobots.RobotsRules robotsFilter;
    private MsgCrawler.FetchInfo.Builder fetchInfoBuilder;

    /** Creates the enqueuer.
     *
     * @param frontier the frontier instantiating the enqueuer.
     * @param rc the configuration to be used.
     */
    FrontierEnqueuer(final Frontier frontier, final RuntimeConfiguration rc) {
      this.frontier = frontier;
      this.classifier = frontier.textClassifier;
      this.scheduleFilter = rc.scheduleFilter;
    }

    /** Initializes the enqueuer for parsing a page with a specific scheme+authority and robots filter.
     *
     * @param crawlRequest the crawl request for which the fetch was done
     * @param robotsFilter the robots filter of the (authority of the) page to be parsed.
     */
    void init( final MsgFrontier.CrawlRequest crawlRequest, final URLRespectsRobots.RobotsRules robotsFilter ) {
      this.source = PulsarHelper.toURI( crawlRequest.getUrlKey() );
      this.robotsFilter = robotsFilter;
      this.fetchInfoBuilder = MsgCrawler.FetchInfo.newBuilder();
    }

    void process( final FetchData fetchData, final ParseData parseData ) {
      process( fetchData );
      process( parseData );
    }

    void flush() {
      frontier.outdegree.add( fetchInfoBuilder.getExternalLinksCount() + fetchInfoBuilder.getInternalLinksCount() );
      frontier.externalOutdegree.add( fetchInfoBuilder.getExternalLinksCount() );
      frontier.enqueue( fetchInfoBuilder.build() );
    }

    private void process( final FetchData fetchData ) {
      fetchInfoBuilder
        .setUrlKey( fetchData.getCrawlRequest().getUrlKey() )
        .setContentLength( (int)fetchData.response().getEntity().getContentLength() )
        .setFetchDuration( (int)(fetchData.endTime - fetchData.startTime) )
        .setFetchTimeToFirstByte((int)(fetchData.firstByteTime - fetchData.startTime))
        .setFetchDate( (int)(fetchData.startTime / (24*60*60*1000)) )
        .setHttpStatus( fetchData.response().getStatusLine().getStatusCode() )
        .setLanguage( fetchData.lang )
        .setIpAddress( ByteString.copyFrom(fetchData.visitState.workbenchEntry.ipAddress) );
    }

    private void process( final ParseData parseData ) {
      if ( parseData.title != null )
        fetchInfoBuilder.setTitle( parseData.title );

      if ( parseData.boilerpipedContent.length() > 0 )
        fetchInfoBuilder.setBody( parseData.boilerpipedContent.toString() );

      fetchInfoBuilder.setParsingErrors(parseData.pageInfo.getHtmlErrorCount() );

      fetchInfoBuilder.getRobotsTagBuilder()
        .setNOINDEX( parseData.pageInfo.getRobotsTagState().contains(RobotsTagState.NOINDEX) )
        .setNOFOLLOW( parseData.pageInfo.getRobotsTagState().contains(RobotsTagState.NOFOLLOW) )
        .setNOARCHIVE( parseData.pageInfo.getRobotsTagState().contains(RobotsTagState.NOARCHIVE) )
        .setNOSNIPPET( parseData.pageInfo.getRobotsTagState().contains(RobotsTagState.NOSNIPPET) );

      if ( parseData.metadata != null )
        for ( final Map.Entry<String,List<String>> meta : parseData.metadata.entries() )
          fetchInfoBuilder.addMetadata( MsgCrawler.Metadata.newBuilder()
            .setKey( meta.getKey() )
            .addAllValues( meta.getValue() ));
      if (  parseData.digest != null )
        fetchInfoBuilder.setContentDigest(ByteString.copyFrom(parseData.digest));

      int linkNum = 0;
      final boolean noFollow = fetchInfoBuilder.getRobotsTagOrBuilder().getNOFOLLOW();
      for ( final HTMLLink link : parseData.links )
        process( link, parseData.baseUri, linkNum++, noFollow );
    }

    private boolean process( final HTMLLink link, final URI base, final int linkNum, final boolean noFollow ) {
      final URI target = getTargetURI( link.uri, base );
      if ( target == null )
        return false;
      try {
        MsgURL.Key urlKey = PulsarHelper.fromURI(target);
        if (urlKey == null)
          return false;
        if (!scheduleFilter.apply(new Link(source, target)))
          return false;
        final MsgLink.LinkInfo.Builder linkInfoBuilder = MsgLink.LinkInfo.newBuilder();
        if (!LinksHelper.trySetLinkInfos(link, linkInfoBuilder, linkNum, noFollow))
          return false;

        final boolean isInternal = isSameHost(source, target); // FIXME: was isSameSchemeAndAuthority
        //final MsgCrawler.CrawlerInfo.Builder crawlerInfoBuilder = MsgCrawler.CrawlerInfo.newBuilder();
        //crawlerInfoBuilder.setIsBlackListed( BlackListing.checkBlacklistedHost(frontier,target) );
        //crawlerInfoBuilder.setDoesRespectRobots( RuntimeConfiguration.FETCH_ROBOTS && robotsFilter != null && isInternal && !URLRespectsRobots.apply(robotsFilter,target) ); // FIXME: wrong !
        //crawlerInfoBuilder.setMatchesScheduleRule( scheduleFilter.apply(new Link(source,target)) ); // FIXME: filtered out above

        MsgCrawler.FetchLinkInfo.Builder fetchLinkInfoBuilder = MsgCrawler.FetchLinkInfo.newBuilder();
        fetchLinkInfoBuilder.setTarget(urlKey);
        fetchLinkInfoBuilder.setLinkInfo(linkInfoBuilder);
        //fetchLinkInfoBuilder.setCrawlerInfo( crawlerInfoBuilder );

        if (isInternal)
          fetchInfoBuilder.addInternalLinks(fetchLinkInfoBuilder);
        else
          fetchInfoBuilder.addExternalLinks(fetchLinkInfoBuilder);
        return true;
      }
      catch (java.net.UnknownHostException uhe) {
        LOGGER.debug(String.format("Warn: UnknownHostException", target.toString()), uhe);
      }
      return false;
    }

    /*
    private TextInfo categorize(final ParseData parseData, final String[] splittedText, TextInfo tinfo) {
      try {
        if (classifier != null) {
          long startClassifTime = System.nanoTime();
          TextInfo extendedTinfo = classifier.predictTokenizedInfo(splittedText, tinfo);
          if (extendedTinfo != null) {
            if (extendedTinfo.gotCategorization()) {
              long endClassifTime = System.nanoTime();
              if (LOGGER.isTraceEnabled())
                LOGGER.trace("content " + Arrays.toString(splittedText) + " lang: [" + extendedTinfo.getLang() + "] text/vocab size: " + tinfo.getTextSize() + "/" + tinfo.getVocabSize());
              LOGGER.debug("Predict time: " + (double) (endClassifTime - startClassifTime) / 1000000000.0 + "s");
              if (LOGGER.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder("categorization: [");
                for (MsgCrawler.Topic topic : tinfo.getCategorization().getTopicList()) {
                  sb.append('(');
                  sb.append(topic.getId());
                  sb.append(", ");
                  sb.append(topic.getScore());
                  sb.append(")");
                }
                sb.append(']');
                LOGGER.debug(sb.toString());
              }
              fetchInfoBuilder.setCategorisation(tinfo.getCategorization());
            }
            if (extendedTinfo.gotEmbedding()) {
              Float[] embedding = extendedTinfo.getEmbedding();
              ByteBuffer bb = ByteBuffer.allocate(embedding.length * 4);
              for (float f : embedding) bb.putFloat(f);
              fetchInfoBuilder.setSemanticVector(ByteString.copyFrom(bb.array()));
            }
          }
          return extendedTinfo;
        }
        else
          return tinfo;
      }
      catch (Exception e) {
        LOGGER.error("Failed to categorize " + parseData.baseUri, e);
        return tinfo;
      }
    }
    */

    private static URI getTargetURI( final String href, final URI base ) {
      final URI target = resolve( href, base );
      return target == null || target.equals(base) ? null : target;
    }

    private static URI resolve( final String href, final URI base ) {
      if ( href.length() == 0 || href.charAt(0) == '#' )
        return base;
      final URI url = BURL.parse( href );
      return url == null ? null : base.resolve( url );
    }

    private static boolean isSameSchemeAndHost( final URI left, final URI right ) {
      return left.getScheme().equals( right.getScheme() ) &&
        left.getHost().equals( right.getHost() );
    }

    private static boolean isSameHost( final URI left, final URI right ) {
      return left.getHost().equals( right.getHost() );
    }
  }

  /** Whether we should stop (used also to reduce the number of threads). */
  public volatile boolean stop;

  /** Sensible format for a double. */
  private final java.text.NumberFormat formatDouble = new java.text.DecimalFormat("#,##0.00");
  /** A reference to the frontier. */
  private final Frontier frontier;
  /** A reference to the store. */
  private final Store store;
  /** The parsers used by this thread. */
  private final ArrayList<Parser<?>> parsers;
  /** A random number generator for the thread */
  private final Random rng;
  /**  */
  private final FrontierEnqueuer frontierLinkReceiver;
  /** A counter for java.nio.BufferOverflowException from Jericho */
  private static int overflowCounter = 0;

  /** Creates a thread.
   *
   * @param frontier the frontier instantiating the thread.
   * @param index the index of this thread (used to give it a name).
   */
  public ParsingThread(final Frontier frontier, final int index) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, IOException, NoSuchAlgorithmException {
    setName(this.getClass().getSimpleName() + '-' + index);
    this.frontier = frontier;
    this.store = frontier.rc.storeClass.getConstructor(RuntimeConfiguration.class).newInstance(frontier.rc);
    this.rng = new Random(index);
    this.frontierLinkReceiver = new FrontierEnqueuer(frontier, frontier.rc);
    this.parsers = new ArrayList<>(frontier.rc.parsers.size());
    for(final Parser<?> parser : frontier.rc.parsers) this.parsers.add(parser.copy());
    frontier.availableFetchData.add(new FetchData(frontier.rc)); // Add extra available Fetch Data
    frontier.fetchDataCount.incrementAndGet();
    setPriority((Thread.NORM_PRIORITY + Thread.MIN_PRIORITY) / 2); // Below main threads
  }

  @Override
  public void run() {
    try {
      LOGGER.warn( "thread [started]" );
      frontier.runningParsingThreads.incrementAndGet();
      while ( !stop ) {
        final FetchData fetchData = getNextFetchData();
        if ( fetchData != null ) {
          frontier.workingParsingThreads.incrementAndGet();
          long startTime = System.currentTimeMillis();
          processFetchData(fetchData);
          long endTime = System.currentTimeMillis();
          frontier.parsingCount.incrementAndGet();
          frontier.parsingDurationTotal.addAndGet(endTime-startTime);
          frontier.workingParsingThreads.decrementAndGet();
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
      close();
      frontier.runningParsingThreads.decrementAndGet();
      LOGGER.warn( "thread [stopped]" );
    }
  }

  private FetchData getNextFetchData() throws InterruptedException {
    frontier.rc.ensureNotPaused();
    FetchData fetchData;
    for( int i=0; (fetchData=frontier.results.poll()) == null; ++i ) {
      if (stop) return null;
      long newSleep = 1 << Math.min(i, 10);
      Thread.sleep( newSleep );
      frontier.rc.ensureNotPaused();
    }
    return fetchData;
  }

  private void processFetchData( final FetchData fetchData ) throws IOException, InterruptedException {
    try { // This try/finally guarantees that we will release the visit state and signal back.
      parseAndStore( frontierLinkReceiver, fetchData );
    }
    finally {
      if (LOGGER.isTraceEnabled()) LOGGER.trace("Releasing visit state {}", fetchData.visitState);
      frontier.done.add( fetchData.visitState );
      // If we need to downsize FetchData, drop this one
      if (frontier.fetchDataCount.get() <= frontier.rc.parsingThreads + frontier.rc.fetchingThreads) {
        frontier.availableFetchData.add(fetchData);
      } else {
        frontier.fetchDataCount.decrementAndGet();
      }
    }
  }

  private void close() {
    try {
      store.close();
      frontier.robotsWarcParallelOutputStream.get().close();
    }
    catch ( IOException e ) {
      LOGGER.error( "Error while closing store", e );
    }
  }

  private void parseAndStore( final FrontierEnqueuer frontierLinkReceiver, final FetchData fetchData ) throws IOException, InterruptedException {
    final RuntimeConfiguration rc = frontier.rc;
    final VisitState visitState = fetchData.visitState;
    if (LOGGER.isTraceEnabled()) LOGGER.trace("Got fetched response for visit state " + visitState);

    if ( fetchData.robots ) {
      frontier.parsingRobotsCount.incrementAndGet();
      frontier.robotsWarcParallelOutputStream.get().write(new HttpResponseWarcRecord(fetchData.uri(), fetchData.response()));
      MutableInt crawlDelay = new MutableInt(0);
      if ((visitState.robotsFilter = URLRespectsRobots.parseRobotsResponse(fetchData, rc.userAgentId, crawlDelay)) == null) {
        // We go on getting/creating a workbench entry only if we have robots permissions.
        visitState.schedulePurge();
        LOGGER.warn("Visit state " + visitState + " killed by null robots.txt");
      }
      visitState.setCrawlDelayMS(crawlDelay.intValue()*1000);
      return;
    }

    final long streamLength = fetchData.response().getEntity().getContentLength();

    ParseData parseData = parse( fetchData );

    if ( parseData == null || parseData.pageInfo.getHtmlErrorCount() > 0 )
      frontier.parsingErrorCount.incrementAndGet();

    if ( parseData == null || parseData.digest == null ) {
      // We don't log for zero-length streams.
      if (streamLength != 0 && LOGGER.isDebugEnabled()) LOGGER.debug("Computing binary digest for " + fetchData.uri());
      // Fallback when all other parsers could not complete digest computation.
      parseData = doParse( fetchData.binaryParser, fetchData );
    }

    if ( parseData == null )
      return; // failure while parsing

    //final boolean isDuplicate = streamLength > 0 && !frontier.digests.addHash(parseData.digest); // Essentially thread-safe; we do not consider zero-content pages as duplicates
    final boolean isDuplicate = false;
    //if (LOGGER.isTraceEnabled()) LOGGER.trace("Decided that for {} isDuplicate={}", fetchData.uri(), isDuplicate);
    fetchData.isDuplicate( isDuplicate );

    frontierLinkReceiver.init( fetchData.getCrawlRequest(), visitState.robotsFilter );
    if ( !isDuplicate && rc.followFilter.apply(fetchData) ) {
      frontierLinkReceiver.process( fetchData, parseData );
      frontierLinkReceiver.flush();
    }
    else {
      LOGGER.debug( "NOT Following {}", fetchData.uri() );
    }

    final String result = store( rc, fetchData, parseData, !isDuplicate, streamLength );

    //if (LOGGER.isDebugEnabled())
    //  LOGGER.debug( "Fetched " + fetchData.uri()
    //    + " (" + Util.formatSize((long)(1000.0 * fetchData.length() / (fetchData.endTime - fetchData.startTime + 1)), formatDouble) + "B/s; "
    //    + frontierLinkReceiver.scheduledLinks + "/" + frontierLinkReceiver.outlinks + "; " + result + ")");
  }

  private ParseData parse( final FetchData fetchData ) {
    if ( !frontier.rc.parseFilter.apply(fetchData) ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "I'm not parsing page " + fetchData.uri() );
      return null;
    }

    final Parser<?> parser = getParser( fetchData );
    if ( parser == null ) {
      if ( LOGGER.isInfoEnabled() ) LOGGER.info( "I'm not parsing page " + fetchData.uri() + " because I could not find a suitable parser" );
      return null;
    }

    final ParseData parseData = doParse( parser, fetchData );

    if ( parseData == null )
      return null;

    if ( parseData.rewritten != null )
      rewriteContentToFetchData( parseData.rewritten, parseData.pageInfo.getGuessedCharset(), fetchData );

    if ( parseData.pageInfo != null ) {
      fetchData.extraMap.putAll( ImmutableMap.of(
        "X-BUbiNG-Charset-Detection-Info", parseData.pageInfo.getCharsetDetectionInfo().toString(),
        "X-BUbiNG-Language-Detection-Info", parseData.pageInfo.getLanguageDetectionInfo().toString(),
        "BUbiNG-Guessed-Meta-Charset", parseData.pageInfo.getCharsetDetectionInfo().htmlMetaCharset,
        "BUbiNG-Guessed-ICU-Charset", parseData.pageInfo.getCharsetDetectionInfo().icuCharset,
        "BUbiNG-Guessed-HTTP-Charset", parseData.pageInfo.getCharsetDetectionInfo().httpHeaderCharset
      ) );
      fetchData.extraMap.put( "BUbiNG-Guessed-Html5", String.valueOf(parseData.pageInfo.isHtmlVersionAtLeast5()) );
      fetchData.extraMap.put( "BUbiNG-Guessed-responsive", String.valueOf(parseData.pageInfo.hasViewportMeta()) );
      if ( parseData.getCharsetName() != null )
        fetchData.extraMap.put( "BUbiNG-Guessed-Charset", parseData.getCharsetName() );
      if ( parseData.getLanguageName() != null ) {
        fetchData.extraMap.put("BUbiNG-Guessed-Language", parseData.getLanguageName() );
        fetchData.lang = LanguageCodes.getByte( parseData.getLanguageName() );
      }
    }

    return parseData;
  }

  private Parser<?> getParser( final FetchData fetchData ) {
    for ( final Parser<?> parser : parsers ) {
      if ( parser.apply(fetchData) )
        return parser;
    }
    return null;
  }

  private ParseData doParse( final Parser<?> parser, final FetchData fetchData ) {
    try {
      return parser.parse( fetchData.uri(), fetchData.response() );
    }
    catch ( final BufferOverflowException e ) {
      LOGGER.warn( "Overflow while parsing {} ({})", fetchData.uri(), ++overflowCounter );
      frontier.parsingExceptionCount.incrementAndGet();
      return null;
    }
    catch ( final Exception e ) {
      // This mainly catches Jericho and network problems
      LOGGER.warn( "Exception while parsing " + fetchData.uri() + " with " + parser, e );
      frontier.parsingExceptionCount.incrementAndGet();
      return null;
    }
  }

  private void rewriteContentToFetchData( final StringBuilder content, final Charset charsetOpt, final FetchData fetchData ) {
    try {
      final HttpResponse response = fetchData.response();
      final InspectableCachedHttpEntity originalEntity = (InspectableCachedHttpEntity) response.getEntity();
      final BasicHttpEntity rewrittenEntity = new BasicHttpEntity();
      final InputStream is = IOUtils.toInputStream( content, charsetOpt != null ? charsetOpt : StandardCharsets.UTF_8 );

      rewrittenEntity.setContent( is );
      rewrittenEntity.setContentType( originalEntity.getContentType() );
      originalEntity.setEntity( rewrittenEntity );
      originalEntity.copyFullContent();
      final long contentLength = originalEntity.getContentLength();
      rewrittenEntity.setContentLength( contentLength );
      // We are cheating with the truth, so we must change the response's header
      response.setHeader("Content-Length", Long.toString(contentLength));
      response.setEntity( originalEntity );
    }
    catch ( IOException e ) {
      LOGGER.error( "Failed to rewrite content of " + fetchData.uri() + " to fetchData", e );
    }
    catch ( UnsupportedOperationException uoe) {
      LOGGER.error( "Failed to rewrite content of " + fetchData.uri() + " to fetchData", uoe);
    }
  }

  private String store( final RuntimeConfiguration rc, final FetchData fetchData, final ParseData parseData, final boolean isNotDuplicate, final long streamLength ) throws IOException, InterruptedException {
    final boolean mustBeStored = rc.storeFilter.apply( fetchData );

    // ALERT: store exceptions should cause shutdown.
    final String result;
    if (mustBeStored) {
      if (isNotDuplicate) {
        final int code = fetchData.response().getStatusLine().getStatusCode() / 100;
        if (code > 0 && code < 6) frontier.archetypesStatus[code].incrementAndGet();
        else frontier.archetypesStatus[0].incrementAndGet();

        if (streamLength >= 0) frontier.contentLength.add(streamLength);

        final Header contentTypeHeader = fetchData.response().getEntity().getContentType();
        if (contentTypeHeader != null) {
          final String contentType = contentTypeHeader.getValue();
          if (StringUtils.startsWithIgnoreCase(contentType, "text")) frontier.contentTypeText.incrementAndGet();
          else if (StringUtils.startsWithIgnoreCase(contentType, "image")) frontier.contentTypeImage.incrementAndGet();
          else if (StringUtils.startsWithIgnoreCase(contentType, "application")) frontier.contentTypeApplication.incrementAndGet();
          else frontier.contentTypeOthers.incrementAndGet();
        }

        result = "stored";
      }
      else {
        frontier.duplicates.incrementAndGet();
        result = "duplicate";
      }
      fetchData.extraMap.put("BUbiNG-Fetching-Duration", Long.toString(fetchData.endTime - fetchData.startTime));
      store.store( fetchData.uri(), fetchData.response(), !isNotDuplicate,
        parseData.digest, parseData.getCharsetName(), parseData.getLanguageName(),
        fetchData.extraMap, parseData.textContent );

    }
    else {
      result = "not stored";
    }

    return result;
  }
}
