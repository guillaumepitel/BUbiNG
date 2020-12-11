package it.unimi.di.law.bubing.frontier.comm;

import com.exensa.util.compression.HuffmanModel;
import com.exensa.wdl.common.EntityHelper;
import com.exensa.wdl.common.Serializer;
import com.exensa.wdl.protobuf.frontier.MsgFrontier;
import com.exensa.wdl.protobuf.url.EnumScheme;
import com.exensa.wdl.protobuf.url.MsgURL;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import it.unimi.di.law.bubing.util.BURL;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;


public class PulsarHelper
{
  final static LoadingCache<ByteString, MsgURL.Key.Builder> hostnameCache =
    CacheBuilder.newBuilder()
    .maximumSize(512*1024)
    .build(
      new CacheLoader<ByteString, MsgURL.Key.Builder>() {
        @Override
        public MsgURL.Key.Builder load(ByteString bytes) throws Exception {
          return _schemeAuthority(bytes.toByteArray());
        }
      }
    );

  public static MsgURL.Key fromURI(final URI uri) throws UnknownHostException, MalformedURLException {
    return Serializer.URL.Key.from(uri);
  }

  public static URI toURI( final MsgURL.URL url ) {
    return URI.create( toString(url) );
  }

  public static URI toURI( final MsgURL.Key urlKey ) {
    return URI.create( toString(urlKey) );
  }

  public static String toString( final MsgURL.Key url ) {
    return Serializer.URL.Key.toString(url);
  }

  public static String toString( final MsgURL.URL url ) {
    return getScheme(url.getScheme()) + "://" + url.getHost() + url.getPathQuery();
  }

  /**
   * Removes the scheme+authority part of a crawl request
   * @param crawlRequest the original crawl request
   * @return serialized stripped crawl request
   */
  public static byte[] toMinimalCrawlRequestSerialized(MsgFrontier.CrawlRequest crawlRequest) {
    MsgFrontier.CrawlRequest.Builder crawlRequestBuilder = crawlRequest.toBuilder();
    MsgURL.Key.Builder urlKeyBuilder = crawlRequestBuilder.getUrlKeyBuilder();
    urlKeyBuilder.clearScheme().clearZDomain().clearZHostPart();
    crawlRequestBuilder.setUrlKey(urlKeyBuilder);
    return  crawlRequestBuilder.build().toByteArray();
  }

  public static MsgFrontier.CrawlRequest minimalCrawlRequestFromPathQuery(String pathQuery) {
    MsgFrontier.CrawlRequest.Builder crawlRequestBuilder = MsgFrontier.CrawlRequest.newBuilder();
    MsgURL.Key.Builder urlKeyBuilder = MsgURL.Key.newBuilder();
    urlKeyBuilder.setZPathQuery(ByteString.copyFrom(toZ(toASCII(pathQuery))));
    return crawlRequestBuilder.setUrlKey(urlKeyBuilder).build();
  }
  public static byte[] schemeAuthority( final MsgURL.KeyOrBuilder urlKey ) {
    return schemeAuthority( Serializer.URL.from(urlKey) );
  }

  public static byte[] schemeAuthority( final MsgURL.URL url ) {
    return toASCII( getScheme(url.getScheme()) + "://" + url.getHost() );
  }

  public static MsgURL.Key.Builder schemeAuthority(final byte[] schemeAuthority) {
    return hostnameCache.getUnchecked(ByteString.copyFrom(schemeAuthority));
  }

  public static MsgURL.Key.Builder _schemeAuthority(final byte[] schemeAuthority) throws UnknownHostException {
    final MsgURL.Key.Builder urlBuilder = MsgURL.Key.newBuilder();

    final String fullHost = BURL.hostFromSchemeAndAuthority(schemeAuthority);
    final EntityHelper.SplittedHost sh = EntityHelper.SplittedHost.fromHostName( fullHost );
    urlBuilder.setZHostPart(Serializer.PathComp.compressStringToByteString(sh.hostPart));
    urlBuilder.setZDomain(Serializer.PathComp.compressStringToByteString(sh.domain));

    final String sa = fromASCII(schemeAuthority);
    if (sa.startsWith("https://"))
      urlBuilder.setScheme(EnumScheme.Enum.HTTPS);
    else
    if (sa.startsWith("http://"))
      urlBuilder.setScheme(EnumScheme.Enum.HTTP);
    else
      urlBuilder.setScheme(EnumScheme.Enum.UNKNOWN);
    return urlBuilder;
  }

  private static EnumScheme.Enum getScheme( final String scheme ) {
    if ( "http".equals(scheme) ) return EnumScheme.Enum.HTTP;
    else if ( "https".equals(scheme) ) return EnumScheme.Enum.HTTPS;
    else return EnumScheme.Enum.UNKNOWN;
  }

  private static String getScheme( final EnumScheme.Enum scheme ) {
    switch ( scheme ) {
      case HTTP : return "http";
      case HTTPS : return "https";
      default: return null;
    }
  }

  public static byte[] toZ( final byte[] ascii ) {
    return HuffmanModel.defaultModel.compress( ascii );
  }

  // FIXME: in most cases, we should use it.unimi.di.law.bubing.util.Util.toByteArray
  public static byte[] toASCII( final String string ) {
    return string.getBytes( StandardCharsets.US_ASCII );
  }

  // FIXME: in most cases, we should use it.unimi.di.law.bubing.util.Util.toString
  public static String fromASCII( final byte[] ascii ) {
    return new String( ascii, StandardCharsets.US_ASCII );
  }
}
