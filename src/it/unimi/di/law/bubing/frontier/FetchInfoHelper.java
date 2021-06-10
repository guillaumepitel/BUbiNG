package it.unimi.di.law.bubing.frontier;

import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.common.UnexpectedException;
import com.exensa.wdl.protobuf.ProtoHelper;
import com.exensa.wdl.protobuf.crawler.EnumFetchStatus;
import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import it.unimi.di.law.bubing.frontier.comm.PulsarHelper;
import org.slf4j.LoggerFactory;

public class FetchInfoHelper {
  private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FetchInfoHelper.class);

  static MsgFrontier.CrawlRequest.Builder createCrawlRequest( final MsgURL.Key schemeAuthority, final byte[] minimalCrawlRequestSerialized ) throws InvalidProtocolBufferException {
    MsgFrontier.CrawlRequest.Builder crawlRequestBuilder = MsgFrontier.CrawlRequest.parseFrom(minimalCrawlRequestSerialized).toBuilder();
    MsgURL.Key.Builder urlKeyBuilder = crawlRequestBuilder.getUrlKeyBuilder();
    urlKeyBuilder
      .setScheme(schemeAuthority.getScheme())
      .setZDomain(schemeAuthority.getZDomain())
      .setZHostPart(schemeAuthority.getZHostPart());
    return crawlRequestBuilder;
  }

  private static MsgCrawler.FetchInfo fetchInfoFailedGeneric( final MsgFrontier.CrawlRequestOrBuilder crawlRequest,
                                                              final VisitState visitState,
                                                              final EnumFetchStatus.Enum fetchStatus ) {
    final MsgCrawler.FetchInfo.Builder fetchInfoBuilder = MsgCrawler.FetchInfo.newBuilder()
      .setUrlKey( crawlRequest.getUrlKey() )
      .setFetchDateDeprecated( ProtoHelper.getDayNow() )
      .setFetchTimeMinutes( ProtoHelper.getMinuteNow() )
      .setFetchStatus( fetchStatus );
    if ( visitState.workbenchEntry != null && visitState.workbenchEntry.ipAddress != null )
      fetchInfoBuilder.setIpAddress( ByteString.copyFrom(visitState.workbenchEntry.ipAddress) );
    return fetchInfoBuilder.build();
  }

  static MsgCrawler.FetchInfo fetchInfoFailedBlackList(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.BLACKLISTED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedFiltered(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.CRAWLER_FILTERED);
  }

  static MsgCrawler.FetchInfo fetchInfoFailedRobots(MsgFrontier.CrawlRequestOrBuilder crawlRequest, VisitState visitState) {
    return fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.ROBOTS_DENIED);
  }

  static void drainVisitStateForError( final Frontier frontier, final VisitState visitState ) throws InterruptedException {
    final MsgURL.Key schemeAuthorityProto = PulsarHelper.schemeAuthority(visitState.schemeAuthority).build();
    LOGGER.info("Draining " + visitState.size() + " crawl request for host " + Serializer.URL.Key.toString(schemeAuthorityProto));
    while ( !visitState.isEmpty() ) {
      frontier.rc.ensureNotPaused();
      final byte[] minimalCrawlRequestSerialized = visitState.dequeue(); // contains a zPathQuery
      if (minimalCrawlRequestSerialized == VisitState.ROBOTS_PATH) // skip for robots.txt
        continue;
      try {
        final MsgFrontier.CrawlRequest.Builder crawlRequest = createCrawlRequest(schemeAuthorityProto, minimalCrawlRequestSerialized);
        frontier.enqueue(fetchInfoFailedGeneric(crawlRequest, visitState, EnumFetchStatus.Enum.HOST_INVALID));
      } catch (InvalidProtocolBufferException ipbe) {
        throw new UnexpectedException(ipbe);
      }
      frontier.fetchingFailedHostCount.incrementAndGet();
      frontier.fetchingFailedCount.incrementAndGet();
    }
  }

}
