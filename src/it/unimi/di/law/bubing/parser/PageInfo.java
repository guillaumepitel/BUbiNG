package it.unimi.di.law.bubing.parser;

import ch.qos.logback.classic.Level;
import it.unimi.di.law.bubing.parser.html.RobotsTagState;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.bubing.util.ByteArrayCharSequence;
import it.unimi.di.law.bubing.util.cld2.Cld2Result;
import it.unimi.di.law.bubing.util.cld2.Cld2Tool;
import it.unimi.di.law.bubing.util.detection.CharsetDetectionInfo;
import it.unimi.di.law.bubing.util.detection.ETagInfo;
import it.unimi.di.law.bubing.util.detection.LanguageDetectionInfo;
import it.unimi.di.law.bubing.util.detection.LocationDetectionInfo;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import it.unimi.dsi.util.TextPattern;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class PageInfo
{
  private static final Logger LOGGER = LoggerFactory.getLogger( PageInfo.class );

  private static final int MIN_CLD2_PAGE_CONTENT = 6;
  private static final int MAX_CLD2_PAGE_CONTENT = 5000;

  private static final Pattern HTTP_EQUIV_PATTERN = Pattern.compile( ".*\\bhttp-equiv\\s*=\\s*(.+)", Pattern.CASE_INSENSITIVE );
  private static final Pattern CONTENT_TYPE_PATTERN = Pattern.compile( "(?:\"content-type\"|'content-type'|content-type\\b)\\s*(.+)", Pattern.CASE_INSENSITIVE );
  private static final Pattern CONTENT_LANGUAGE_PATTERN = Pattern.compile( "(?:\"content-language\"|'content-language'|content-language\\b)\\s*(.+)", Pattern.CASE_INSENSITIVE );
  private static final Pattern REFRESH_PATTERN = Pattern.compile( "(?:\"refresh\"|'refresh'|refresh\\b)\\s*(.+)", Pattern.CASE_INSENSITIVE );
  private static final Pattern CONTENT_PATTERN = Pattern.compile( "\\s*content\\s*=\\s*(?<quote>['\"])(.+)\\k<quote>.*", Pattern.CASE_INSENSITIVE );
  private static final Pattern ROBOTS_PATTERN = Pattern.compile( ".*\\bname\\s*=\\s*(?:\"robots\"|'robots'|robots\\b)\\s*(.+)", Pattern.CASE_INSENSITIVE );
  private static final Pattern VIEWPORT_PATTERN = Pattern.compile( ".*\\bname\\s*=\\s*(?:\"viewport\"|'viewport'|viewport\\b).*", Pattern.CASE_INSENSITIVE );
  private static final Pattern REFRESH_CONTENT_PATTERN = Pattern.compile( ".*URL\\s*=\\s*(?<quote>['\"]?)(.+)\\k<quote>", Pattern.CASE_INSENSITIVE );
  private static final Pattern CHARSET_PATTERN = Pattern.compile( ".*\\bcharset\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
  private static final Pattern LANG_PATTERN = Pattern.compile( ".*\\blang\\s*=\\s*\"?([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=\"]]+).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL );
  private static final TextPattern DOCTYPE_PATTERN = new TextPattern( "<!doctype", TextPattern.CASE_INSENSITIVE );
  private static final TextPattern HTML_PATTERN = new TextPattern( "<html", TextPattern.CASE_INSENSITIVE );
  private static final TextPattern META_PATTERN = new TextPattern( "<meta", TextPattern.CASE_INSENSITIVE );

  private final URI uri;
  private final HtmlCharsetDetector charsetDetector;
  private final CharsetDetectionInfo charsetDetectionInfo;
  private final LanguageDetectionInfo languageDetectionInfo;
  private final LocationDetectionInfo locationDetectionInfo;
  private final RobotsTagState robotsTagState;
  private final ETagInfo eTagInfo;
  private final ArrayList<HTMLLink> headerLinks;
  private final ArrayList<HTMLLink> links;
  private Charset guessedCharset;
  private Locale guessedLanguage;
  private URI guessedLocation;
  private boolean hasViewportMeta;
  private boolean htmlVersionAtLeast5;
  private int htmlErrorCount;

  // accessors ---------------------------------------------------------------------------------------------------------------------

  public CharsetDetectionInfo getCharsetDetectionInfo() {
    return charsetDetectionInfo;
  }

  public LanguageDetectionInfo getLanguageDetectionInfo() {
    return languageDetectionInfo;
  }

  public LocationDetectionInfo getLocationDetectionInfo() {
    return locationDetectionInfo;
  }

  public RobotsTagState getRobotsTagState() {
    return robotsTagState;
  }

  public List<HTMLLink> getHeaderLinks() {
    return headerLinks;
  }

  public List<HTMLLink> getLinks() {
    return links;
  }

  public Charset getGuessedCharset() {
    return guessedCharset;
  }

  public Locale getGuessedLanguage() {
    return guessedLanguage;
  }

  public String getETag() { return eTagInfo.httpHeaderETag != null ? eTagInfo.httpHeaderETag : eTagInfo.htmlMetaETag; }

  public URI getGuessedLocation() {
    return guessedLocation;
  }

  public boolean hasViewportMeta() {
    return hasViewportMeta;
  }

  public boolean isHtmlVersionAtLeast5() {
    return htmlVersionAtLeast5;
  }

  public List<HTMLLink> getRedirectLinks() {
    return java.util.stream.Stream.of(
      locationDetectionInfo.httpHeaderLocation,
      locationDetectionInfo.htmlRefreshLocation,
      locationDetectionInfo.httpHeaderRefreshLocation
      )
      .filter( Objects::nonNull )
      .map( (uri) -> new HTMLLink( HTMLLink.Type.REDIRECT, uri.toString(), null, null, null ) )
      .collect( java.util.stream.Collectors.toList() );
  }

  public int getHtmlErrorCount() {
    return htmlErrorCount;
  }

  public void setHtmlErrorCount( final int errorCount ) {
    htmlErrorCount = errorCount;
  }

  // contructor --------------------------------------------------------------------------------------------------------------------

  public PageInfo( final URI uri ) {
    this( uri, new HtmlCharsetDetector(0) );
  }

  public PageInfo( final URI uri, final HtmlCharsetDetector charsetDetector ) {
    this.uri = uri;
    this.charsetDetector = charsetDetector;
    this.charsetDetectionInfo = new CharsetDetectionInfo();
    this.languageDetectionInfo = new LanguageDetectionInfo();
    this.locationDetectionInfo = new LocationDetectionInfo();
    this.eTagInfo = new ETagInfo();
    this.robotsTagState = new RobotsTagState();
    this.headerLinks = new ArrayList<>();
    this.links = new ArrayList<>();
    this.guessedCharset = StandardCharsets.UTF_8;
    this.guessedLanguage = null;
    this.guessedLocation = uri;
    this.hasViewportMeta = false;
    this.htmlVersionAtLeast5 = false;
    this.htmlErrorCount = 0;
  }

  // API ---------------------------------------------------------------------------------------------------------------------------

  public void extractFromHttpHeader( final HttpResponse httpResponse ) {
    final boolean dummy =
      tryExtractCharsetFromHeader( httpResponse ) |
      tryExtractLanguageFromHeader( httpResponse ) |
      tryExtractLocationFromHeader( httpResponse ) |
      tryExtractETagFromHeader( httpResponse ) |
      tryExtractContentLocationFromHeader( httpResponse ) |
      tryExtractRefreshFromHeader( httpResponse ) |
      tryExtractRobotsTagFromHeader( httpResponse ) |
      tryExtractLinksFromHeader( httpResponse );
  }

  public void extractFromMetas( final HttpResponse httpResponse ) throws IOException {
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
    final List<CharSequence> allMetaEntries = getAllMetaEntries( contentStream.buffer, contentStream.inspectable );
    for ( final CharSequence meta : allMetaEntries ) {
      final boolean dummy =
        tryExtractHttpEquivFromMeta( meta ) ||
        tryExtractCharsetFromMeta( meta ) ||
        tryExtractViewportFromMeta( meta ) ||
        tryExtractRobotsFromMeta( meta );
    }
  }

  public void extractFromHtml( final HttpResponse httpResponse, final char[] buffer ) throws IOException {
    final boolean dummy =
      tryExtractHtmlVersion( httpResponse ) |
      tryExtractLanguageFromHtml( httpResponse ) |
      tryGuessCharsetFromHtml( httpResponse, buffer );
  }

  public void extractFromContent( final CharSequence content ) {
    final boolean dummy =
      tryGuessLanguageFromContent( content );
  }

  // implementation ----------------------------------------------------------------------------------------------------------------

  private boolean tryExtractCharsetFromHeader( final HttpResponse httpResponse ) {
    // TODO: check if it will make sense to use httpResponse.getLocale()
    final Header contentTypeHeader = httpResponse.getEntity().getContentType();
    if ( contentTypeHeader == null ) return false;
    final String charsetName = getCharsetNameFromHeader( contentTypeHeader.getValue() );
    if ( charsetName == null ) return false;
    charsetDetectionInfo.httpHeaderCharset = charsetName;
    return true; // FIXME: as original code, do not set guessedCharset from HTTP HEADER
    //return trySetGuessedCharsetFrom( charsetName, "in HTTP HEADER" );
  }

  private boolean tryExtractLanguageFromHeader( final HttpResponse httpResponse ) {
    final Header contentLanguageHeader = httpResponse.getFirstHeader( "Content-Language" );
    if ( contentLanguageHeader == null ) return false;
    final String languageTag = contentLanguageHeader.getValue();
    if ( languageTag == null ) return false;
    languageDetectionInfo.httpHeaderLanguage = languageTag;
    return trySetGuessedLanguageFrom( languageTag, "in HTTP HEADER" );
  }

  private boolean tryExtractLocationFromHeader( final HttpResponse httpResponse ) {
    final Header locationHeader = httpResponse.getFirstHeader( "Location" );
    if ( locationHeader == null ) return false;
    final String location = locationHeader.getValue();
    if ( location == null ) return false;
    locationDetectionInfo.httpHeaderLocation = trySetGuessedLocationFrom( "Location", location, "in HTTP HEADER" );
    return locationDetectionInfo.httpHeaderLocation != null;
  }

  private boolean tryExtractETagFromHeader( final HttpResponse httpResponse ) {
    final Header eTagHeader = httpResponse.getFirstHeader( "ETag" );
    if ( eTagHeader == null ) return false;
    final String eTag = eTagHeader.getValue();
    if ( eTag == null ) return false;
    eTagInfo.httpHeaderETag = eTag;
    return eTagInfo.httpHeaderETag != null;
  }

  private boolean tryExtractContentLocationFromHeader( final HttpResponse httpResponse ) {
    final Header contentLocationHeader = httpResponse.getFirstHeader( "Content-Location" );
    if ( contentLocationHeader == null ) return false;
    final String location = contentLocationHeader.getValue();
    if ( location == null ) return false;
    locationDetectionInfo.httpHeaderContentLocation = trySetGuessedLocationFrom( "Content-Location", location, "in HTTP HEADER" );
    return locationDetectionInfo.httpHeaderContentLocation != null;
  }

  private boolean tryExtractRefreshFromHeader( final HttpResponse httpResponse ) {
    final Header contentRefreshHeader = httpResponse.getFirstHeader( "Refresh" );
    if ( contentRefreshHeader == null ) return false;
    final String refresh = contentRefreshHeader.getValue();
    if ( refresh == null ) return false;
    final Matcher mContent = REFRESH_CONTENT_PATTERN.matcher( refresh );
    if ( !mContent.matches() ) return false;
    final String location = mContent.group(2);
    locationDetectionInfo.httpHeaderRefreshLocation = trySetGuessedLocationFrom( "Refresh", location, "in HTTP HEADER" );
    return locationDetectionInfo.httpHeaderRefreshLocation != null;
  }

  private boolean tryExtractRobotsTagFromHeader( final HttpResponse httpResponse ) {
    final Header[] headers = httpResponse.getHeaders( "X-Robots-Tag" );
    if ( headers.length == 0 ) return false;
    for ( final Header h : headers )
      robotsTagState.add( h.getValue() );
    return true;
  }

  private boolean tryExtractLinksFromHeader( final HttpResponse httpResponse ) {
    final Header[] headers = httpResponse.getHeaders( "Link" );
    if ( headers.length == 0 ) return false;
    for ( final Header h : headers ) {
      final Iterable<HTMLLink> links = LinksHelper.fromHttpHeader( h.getValue() );
      for ( final HTMLLink link : links )
        headerLinks.add( link );
    }
    return true;
  }

  private boolean tryExtractHttpEquivFromMeta( final CharSequence meta ) {
    final Matcher mHttpEquiv = HTTP_EQUIV_PATTERN.matcher( meta );
    if ( !mHttpEquiv.matches() ) return false;
    final String httpEquiv = mHttpEquiv.group(1);
    if ( httpEquiv == null ) return false;
    return tryExtractCharsetFromHttpEquiv( httpEquiv ) ||
      tryExtractETagFromHttpEquiv( httpEquiv ) ||
      tryExtractLanguageFromHttpEquiv( httpEquiv ) ||
      tryExtractRefreshFromHttpEquiv( httpEquiv );
  }

  private boolean tryExtractCharsetFromMeta( final CharSequence meta ) {
    final String charsetName = getCharsetNameFromHeader( meta );
    if ( charsetName == null ) return false;
    charsetDetectionInfo.htmlMetaCharset = charsetName;
    return trySetGuessedCharsetFrom( charsetName, "in META CHARSET" );
  }

  private boolean tryExtractViewportFromMeta( final CharSequence meta ) {
    if ( !VIEWPORT_PATTERN.matcher(meta).matches() )
      return false;
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found viewport in META of {}", uri.toString() );
    hasViewportMeta = true;
    return true;
  }

  private boolean tryExtractRobotsFromMeta( final CharSequence meta ) {
    final Matcher mRobots = ROBOTS_PATTERN.matcher( meta );
    if ( !mRobots.matches() ) return false;
    final Matcher mContent = CONTENT_PATTERN.matcher( mRobots.group(1) );
    if ( !mContent.matches() ) return false;
    final String robotsTags = mContent.group(2);
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found robots {} in META of {}", robotsTags, uri.toString() );
    robotsTagState.add( robotsTags );
    return true;
  }

  private boolean tryExtractCharsetFromHttpEquiv( final String httpEquiv ) {
    final Matcher mContentType = CONTENT_TYPE_PATTERN.matcher( httpEquiv );
    if ( !mContentType.matches() ) return false;
    final Matcher mContent = CONTENT_PATTERN.matcher( mContentType.group(1) );
    if ( !mContent.matches() ) return false;
    final String charsetName = getCharsetNameFromHeader( mContent.group(2) );
    if ( charsetName == null ) return false;
    charsetDetectionInfo.htmlMetaCharset = charsetName;
    return trySetGuessedCharsetFrom( charsetName, "in META HTTP-EQUIV" );
  }

  private boolean tryExtractETagFromHttpEquiv(final String httpEquiv ) {
    final Matcher mContentType = CONTENT_TYPE_PATTERN.matcher( httpEquiv );
    if ( !mContentType.matches() ) return false;
    final Matcher mContent = CONTENT_PATTERN.matcher( mContentType.group(1) );
    if ( !mContent.matches() ) return false;
    final String eTag = mContent.group(2);
    if ( eTag == null ) return false;
    eTagInfo.htmlMetaETag = eTag;
    return true;
  }

  private boolean tryExtractLanguageFromHttpEquiv( final String httpEquiv ) {
    final Matcher mContentLanguage = CONTENT_LANGUAGE_PATTERN.matcher( httpEquiv );
    if ( !mContentLanguage.matches() ) return false;
    final Matcher mContent = CONTENT_PATTERN.matcher( mContentLanguage.group(1) );
    if ( !mContent.matches() ) return false;
    final String languageTag = mContent.group(2);
    languageDetectionInfo.htmlLanguage = languageTag;
    return trySetGuessedLanguageFrom( languageTag, "in META HTTP-EQUIV" );
  }

  private boolean tryExtractRefreshFromHttpEquiv( final String httpEquiv ) {
    final Matcher mHttpEquiv = REFRESH_PATTERN.matcher( httpEquiv );
    if ( !mHttpEquiv.matches() ) return false;
    final Matcher mContent = CONTENT_PATTERN.matcher( mHttpEquiv.group(1));
    if ( !mContent.matches() ) return false;
    final Matcher mRefresh = REFRESH_CONTENT_PATTERN.matcher( mContent.group(2) );
    if ( !mRefresh.matches() ) return false;
    final String location = mRefresh.group(2);
    locationDetectionInfo.htmlRefreshLocation = trySetGuessedLocationFrom( "Refresh", location, "in META HTTP-EQUIV" );
    return locationDetectionInfo.htmlRefreshLocation != null;
  }

  private boolean tryExtractHtmlVersion( final HttpResponse httpResponse ) throws IOException {
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
    final String docType = getDocType( contentStream.buffer, contentStream.inspectable );
    if ( docType == null ) return false;
    htmlVersionAtLeast5 = docType.equalsIgnoreCase( "html" );
    return htmlVersionAtLeast5;
  }

  private boolean tryExtractLanguageFromHtml( final HttpResponse httpResponse ) throws IOException {
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
    final String languageName = getLanguageName( contentStream.buffer, contentStream.inspectable );
    if ( languageName == null ) return false;
    return trySetGuessedLanguageFrom( languageName, "in HTML TAG" );
  }

  private boolean tryGuessCharsetFromHtml( final HttpResponse httpResponse, final char[] buffer ) throws IOException {
    if ( guessedCharset != null ) return false; // FIXME: see tryExtractCharsetFromHeader
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) httpResponse.getEntity().getContent();
    final String charsetName = charsetDetector.detect( contentStream.buffer, contentStream.inspectable, buffer );
    if ( charsetName == null ) return false;
    charsetDetectionInfo.icuCharset = charsetName;
    return trySetGuessedCharsetFrom( charsetName, "with ICU" );
  }

  private boolean tryGuessLanguageFromContent( final CharSequence content ) {
    if ( content.length() < MIN_CLD2_PAGE_CONTENT )
      return false;
    final String tld = uri.getHost().substring( uri.getHost().lastIndexOf('.') + 1 );
    final String hint = guessedLanguage == null ? null : guessedLanguage.getLanguage();
    final Cld2Result result = Cld2Tool.detect( content, MAX_CLD2_PAGE_CONTENT, tld, hint ); // TODO : use encoding hints see https://github.com/CLD2Owners/cld2/blob/master/public/encodings.h
    if ( LOGGER.isTraceEnabled() ) LOGGER.trace( "Raw text submitted to language detection is {}", content.toString() );
    languageDetectionInfo.cld2Language = result.code;
    if ( !result.language.equals("Unknown") )
      return trySetGuessedLanguageFrom( result.code, "from CONTENT" );
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Unable to guess language for {}", uri );
    return false;
  }

  private boolean trySetGuessedCharsetFrom( final String charsetName, final String from ) {
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug("Found charset {} {} of {}", charsetName, from, uri.toString() );
    try {
      guessedCharset = Charset.forName( charsetName );
      return true;
    }
    catch ( IllegalCharsetNameException|UnsupportedCharsetException e ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Charset {} found {} is not supported", charsetName, from );
      return false;
    }
  }

  private boolean trySetGuessedLanguageFrom( final String languageTag, final String from ) {
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found language {} {} of {}", languageTag, from, uri.toString() );
    try {
      guessedLanguage = new Locale.Builder().setLanguageTag( languageTag ).build();
      return true;
    }
    catch ( IllformedLocaleException e ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Language {} found {} is not supported", languageTag, from );
      return false;
    }
  }

  private URI trySetGuessedLocationFrom( final String that, final String value, final String from ) {
    if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "Found {} {} {} of {}", that, value, from, uri.toString() );
    final URI validUri = BURL.parse( value );
    if ( validUri == null ) {
      if ( LOGGER.isDebugEnabled() ) LOGGER.debug( "{} {} found {} is not valid", that, value, from );
      return null;
    }
    guessedLocation = uri.resolve( validUri );
    return guessedLocation;
  }

  // tools -----------------------------------------------------------------------------------------------------------------------

  private static String getDocType( final byte buffer[], final int length ) {
    int start = DOCTYPE_PATTERN.search( buffer, 0, length );
    if ( start == -1 ) return null;
    int end = start;
    while ( end < length && buffer[end] != '>' ) end += 1; // Look for closing '>'
    final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(
      buffer,
      start + DOCTYPE_PATTERN.length(),
      end-start - DOCTYPE_PATTERN.length()
    );
    return tagContent.toString().trim();
  }

  private static String getLanguageName( final byte buffer[], final int length ) {
    int start = HTML_PATTERN.search( buffer, 0, length );
    if ( start == -1 ) return null;
    int end = start;
    while ( end < length && buffer[end] != '>' ) end += 1; // Look for closing '>'
    final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(
      buffer,
      start + HTML_PATTERN.length(),
      end-start - HTML_PATTERN.length()
    );
    return getLanguageNameFromHTML( tagContent );
  }

  private static List<CharSequence> getAllMetaEntries( final byte buffer[], final int length ) {
    final ArrayList<CharSequence> metas = new ArrayList<>();
    int start = 0;
    while ( (start=META_PATTERN.search(buffer,start,length)) != -1 ) {
      int end = start;
      while ( end < length && buffer[end] != '>' ) end += 1; // Look for closing '>'
      if ( end == length ) return metas; // No closing '>'
      metas.add( new ByteArrayCharSequence(
        buffer,
        start + META_PATTERN.length(),
        end-start - META_PATTERN.length()
      ) );
      start = end + 1;
    }
    return metas;
  }

  private static String getCharsetNameFromHeader( final CharSequence headerValue ) {
    final Matcher m = CHARSET_PATTERN.matcher( headerValue );
    if ( m.matches() ) {
      final String s = m.group(1);
      int start = 0, end = s.length();
      // TODO: we discard delimiting single/double quotes; is it necessary?
      if ( end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'') ) start = 1;
      if ( end > 0 && (s.charAt(end-1) == '\"' || s.charAt(end-1) == '\'') ) end -= 1;
      if ( start < end ) return s.substring( start, end );
    }
    return null;
  }

  private static String getLanguageNameFromHTML( final CharSequence headerValue ) {
    final Matcher m = LANG_PATTERN.matcher(headerValue);
    if ( m.matches() ) {
      final String s = m.group(1);
      int start = 0, end = s.length();
      // TODO: we discard delimiting single/double quotes; is it necessary?
      if ( end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'') ) start = 1;
      if ( end > 0 && (s.charAt(end-1) == '\"' || s.charAt(end-1) == '\'')) end -= 1;
      if ( start < end ) return s.substring( start, end );
    }
    return null;
  }

  public static void main( final String[] args ) {
    //final var text = "<meta name='robots' content='index, follow, noarchive' />";
    //final var text = "<meta name=\"viewport\" content=\"pouet\" />";
    //final var text = "<meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">";
    //final var text = "<meta http-equiv=\"refresh\" content=\"1; URL=/rss/\">";
    //final var text = "<meta http-equiv=\"refresh\" content=\"1 URL='/rss/'\">";
    final var text = "<meta http-equiv=\"refresh\" content=\"1,URL='/rss/'\">";

    final var bytes = text.getBytes( StandardCharsets.UTF_8 );
    //final var pageInfo = new PageInfo( URI.create("https://www.lemonde.fr/") );
    final var pageInfo = new PageInfo( URI.create("https://www.apple.com/de/rss/") );
    final var entries = PageInfo.getAllMetaEntries( bytes, bytes.length );
    ch.qos.logback.classic.Logger l = (ch.qos.logback.classic.Logger) LOGGER;
    l.setLevel(Level.DEBUG);
    for ( final var seq : entries ) {
      final var meta = seq.toString();
      LOGGER.debug("Processing {}", meta);
      final Matcher mRobots = ROBOTS_PATTERN.matcher(meta);
      if (mRobots.matches()) {
        final var group = mRobots.group(1);
        final Matcher mContent = CONTENT_PATTERN.matcher(mRobots.group(1));
        if (!mContent.matches()) continue;
        final String robotsTags = mContent.group(1);
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Found robots {} in META of {}", robotsTags, pageInfo.uri.toString());
      }
      final Matcher mHTTPEquiv = HTTP_EQUIV_PATTERN.matcher(meta);
      if (mHTTPEquiv.matches()) {
        LOGGER.debug("Processing meta {}", mHTTPEquiv.group(1));
        final Matcher mRefresh = REFRESH_PATTERN.matcher(mHTTPEquiv.group(1));
        if (!mRefresh.matches()) continue;
        final Matcher mContent = CONTENT_PATTERN.matcher(mRefresh.group(1));
        if (!mContent.matches()) continue;
        LOGGER.debug("Processing refresh {}", mContent.group(2));
        final Matcher mRefreshContent = REFRESH_CONTENT_PATTERN.matcher(mContent.group(2));
        if (!mRefreshContent.matches()) continue;
        final String location = mRefreshContent.group(2);
        LOGGER.debug("Found refresh {} in META of {}", location, pageInfo.uri.toString());
      }
    }
  }
}
