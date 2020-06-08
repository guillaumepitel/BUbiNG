package it.unimi.di.law.bubing.parser;

import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import com.kohlschutter.boilerpipe.extractors.KeepEverythingExtractor;
import it.unimi.di.law.bubing.parser.html.*;
import it.unimi.di.law.bubing.util.BURL;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.io.InspectableFileCachedInputStream;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.StreamedSource;
import org.apache.http.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


public final class XHTMLParser implements Parser<Void>
{
  private static final Logger LOGGER = LoggerFactory.getLogger( XHTMLParser.class );
  private static final int CHAR_BUFFER_SIZE = 1024 * 1024; // The size of the internal Jericho buffer.
  private static final int MAX_CHARSET_PAGE_CONTENT = 5000; // The max required amount of page content (without HTML entities) for charset detection
  private static final int REWRITTEN_INITIAL_CAPACITY = 128 * 1024;
  private static final int MAX_ANCHOR_TEXT_LENGTH = 1024;
  private static final int MAX_BODY_LENGTH = 64 * 1024;

  private final char[] buffer;
  private final DigestAppendable digestAppendable;
  private final PureTextAppendable pureTextAppendable;
  private final HtmlCharsetDetector charsetDetector;
  private final StringBuilder rewritten;
  private PageInfo pageInfo;
  private Metadata metadata;

  public XHTMLParser( final String dummy ) {
    this.buffer = new char[ CHAR_BUFFER_SIZE ];
    this.digestAppendable = new DigestAppendable();
    this.pureTextAppendable = new PureTextAppendable();
    this.charsetDetector = new HtmlCharsetDetector( MAX_CHARSET_PAGE_CONTENT );
    this.rewritten = new StringBuilder( REWRITTEN_INITIAL_CAPACITY );
    this.pageInfo = null;
    this.metadata = null;
  }

  @Override
  public ParseData parse( final URI uri, final HttpResponse httpResponse ) throws IOException {
    if (httpResponse.getStatusLine().getStatusCode()/100 == 2)
      return parse2XX(uri, httpResponse);
    else
      return parseXXX(uri, httpResponse);
  }

  private ParseData parse2XX( final URI uri, final HttpResponse httpResponse ) throws IOException {
    init( uri );

    pageInfo.extractFromHttpHeader( httpResponse );
    pageInfo.extractFromMetas( httpResponse );
    pageInfo.extractFromHtml( httpResponse, buffer );

    final HttpEntity entity = httpResponse.getEntity();
    final InspectableFileCachedInputStream contentStream = (InspectableFileCachedInputStream) entity.getContent();

    final HtmlDigestContentHandler digestContentHandler = new HtmlDigestContentHandler( digestAppendable );
    final HtmlPureTextContentHandler pureTextContentHandler = new HtmlPureTextContentHandler( pureTextAppendable );
    final HtmlBoilerpipeHandler boilerpipeHandler = new HtmlBoilerpipeHandler( KeepEverythingExtractor.INSTANCE, MAX_BODY_LENGTH );
    final ContentHandler htmlContentHandler = new HtmlTeeContentHandler( digestContentHandler, pureTextContentHandler, boilerpipeHandler );
    final LinksHandler linksHandler = new LinksHandler( pageInfo.getLinks(), MAX_ANCHOR_TEXT_LENGTH );
    final XhtmlContentHandler xhtmlContentHandler = new XhtmlContentHandler( metadata, htmlContentHandler, linksHandler );
    final JerichoToXhtml jerichoToXhtml = new JerichoToXhtml( xhtmlContentHandler );
    final JerichoHtmlBackup jerichoHtmlBackup = new JerichoHtmlBackup( rewritten );

    final StreamedSource streamedSource = new StreamedSource(new InputStreamReader( contentStream, pageInfo.getGuessedCharset() ));
    streamedSource.setBuffer( buffer );
    final JerichoLoggerWrapper loggerWrapper = new JerichoLoggerWrapper( streamedSource.getLogger() );
    streamedSource.setLogger( loggerWrapper );
    final JerichoParser jerichoParser = new JerichoParser( streamedSource, new JerichoParser.TeeHandler(jerichoToXhtml,jerichoHtmlBackup) );

    try {
      jerichoParser.parse();
    }
    catch ( JerichoParser.ParseException e ) {
      LOGGER.warn( "Failed to parse HTML from " + uri.toString(), e );
      return null;
    }

    pageInfo.setHtmlErrorCount( loggerWrapper.getTagRejected() + loggerWrapper.getTagNotRegistered() );
    pageInfo.extractFromContent( pureTextContentHandler.pureTextAppendable.getContent() );
    updateDigestForRedirection( httpResponse );

    final List<HTMLLink> allLinks = new ArrayList<>();
    allLinks.addAll( pageInfo.getHeaderLinks() );
    allLinks.addAll( pageInfo.getRedirectLinks() );
    allLinks.addAll( pageInfo.getLinks() );

    final URI baseUriOpt = linksHandler.getBaseOpt() == null ? null : BURL.parse( linksHandler.getBaseOpt() );
    final URI baseUri = baseUriOpt == null ? uri : uri.resolve( baseUriOpt );

    return new ParseData(
      baseUri,
      metadata.get( "title" ),
      pageInfo,
      metadata,
      digestAppendable.digest(),
      pureTextAppendable.getContent(),
      boilerpipeHandler.getContent(),
      rewritten,
      allLinks
    );
  }


  private ParseData parseXXX( final URI uri, final HttpResponse httpResponse ) throws IOException {
    init( uri );

    pageInfo.extractFromHttpHeader( httpResponse );
    pageInfo.extractFromMetas( httpResponse );
    pageInfo.extractFromHtml( httpResponse, buffer );

    updateDigestForRedirection( httpResponse );
    final List<HTMLLink> allLinks = new ArrayList<>();
    allLinks.addAll( pageInfo.getHeaderLinks() );
    allLinks.addAll( pageInfo.getRedirectLinks() );
    allLinks.addAll( pageInfo.getLinks() );

    final URI baseUri = uri;

    return new ParseData(
      baseUri,
      metadata.get( "title" ),
      pageInfo,
      metadata,
      digestAppendable.digest(),
      pureTextAppendable.getContent(),
      new StringBuilder(),
      rewritten,
      allLinks
    );
  }

  @Override
  public Void result() {
    return null;
  }

  @Override
  public Parser<Void> copy() {
    return new XHTMLParser( null );
  }

  @Override
  public boolean apply( @Nullable URIResponse uriResponse ) {
    final Header contentType = uriResponse.response().getEntity().getContentType();
    final int status = uriResponse.response().getStatusLine().getStatusCode();
    final boolean isRedirect = ((status / 100) == 3);
    return isRedirect || (contentType != null && contentType.getValue().startsWith("text/html"));
  }

  // implementation ----------------------------------------------------------------------------------------------------------------

  private void init( final URI uri ) {
    pageInfo = new PageInfo( uri, charsetDetector );
    metadata = new Metadata();
    digestAppendable.init( uri );
    pureTextAppendable.init();
    rewritten.setLength( 0 );
  }

  private void updateDigestForRedirection( final HttpResponse httpResponse ) {
    // This is to avoid collapsing 3xx pages with boilerplate content (as opposed to 0-length content).
    if ( httpResponse.getStatusLine().getStatusCode()/100 == 3 ) {
      digestAppendable.append( (char)0 );
      if ( pageInfo.getLocationDetectionInfo().httpHeaderLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().httpHeaderLocation.toString() );
      digestAppendable.append( (char)0 );
      if ( pageInfo.getLocationDetectionInfo().htmlRefreshLocation != null )
        digestAppendable.append( pageInfo.getLocationDetectionInfo().htmlRefreshLocation.toString() );
      digestAppendable.append( (char)0 );
    }
  }

  // inner classes -----------------------------------------------------------------------------------------------------------------

  public static final class HtmlDigestContentHandler implements ContentHandler
  {
    private final DigestAppendable digestAppendable;

    HtmlDigestContentHandler( final DigestAppendable digestAppendable ) {
      this.digestAppendable = digestAppendable;
    }

    @Override
    public void startElement( final String uri, final String localName, final String qName, final Attributes atts ) {
      digestAppendable.startTag( localName );
      if ( localName == HTMLElementName.IFRAME || localName == HTMLElementName.FRAME ) {
        final String src = atts.getValue( "src" );
        if ( src != null ) {
          digestAppendable.append( '\"' );
          digestAppendable.append( src );
          digestAppendable.append( '\"' );
        }
      }
    }

    @Override
    public void endElement( final String uri, final String localName, final String qName ) {
      digestAppendable.endTag( localName );
    }

    @Override
    public void characters( final char[] ch, final int start, final int length ) {
      digestAppendable.append( ch, start, length );
    }

    @Override
    public void ignorableWhitespace( final char[] ch, final int start, final int length ) {
      characters( ch, start, length );
    }

    @Override public void setDocumentLocator( final Locator locator ) { }
    @Override public void startDocument() { }
    @Override public void endDocument() { }
    @Override public void startPrefixMapping( final String prefix, final String uri ) { }
    @Override public void endPrefixMapping( final String prefix ) { }
    @Override public void processingInstruction( final String target, final String data ) { }
    @Override public void skippedEntity( final String name ) { }
  }

  public static final class HtmlPureTextContentHandler implements ContentHandler
  {
    private final PureTextAppendable pureTextAppendable;

    HtmlPureTextContentHandler( final PureTextAppendable pureTextAppendable ) {
      this.pureTextAppendable = pureTextAppendable;
    }

    @Override
    public void characters( final char[] ch, final int start, final int length ) {
      pureTextAppendable.append( ch, start, length );
    }

    @Override
    public void ignorableWhitespace( final char[] ch, final int start, final int length ) {
      characters( ch, start, length );
    }

    @Override public void startElement( final String uri, final String localName, final String qName, final Attributes atts ) { }
    @Override public void endElement( final String uri, final String localName, final String qName ) { }
    @Override public void setDocumentLocator( final Locator locator ) { }
    @Override public void startDocument() { }
    @Override public void endDocument() { }
    @Override public void startPrefixMapping( final String prefix, final String uri ) { }
    @Override public void endPrefixMapping( final String prefix ) { }
    @Override public void processingInstruction( final String target, final String data ) { }
    @Override public void skippedEntity( final String name ) { }
  }

  public static final class HtmlTeeContentHandler implements ContentHandler
  {
    private final ContentHandler[] handlers;

    HtmlTeeContentHandler( final ContentHandler... handlers ) {
      this.handlers = handlers;
    }

    HtmlTeeContentHandler( final Collection<ContentHandler> handlers ) {
      this.handlers = new ContentHandler[ handlers.size() ];
      handlers.toArray( this.handlers );
    }

    @Override
    public void setDocumentLocator( final Locator locator ) {
      for ( final ContentHandler handler : handlers )
        handler.setDocumentLocator( locator );
    }

    @Override
    public void startDocument() throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.endDocument();
    }

    @Override
    public void startPrefixMapping( final String prefix, final String uri ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.startPrefixMapping( prefix, uri );
    }

    @Override
    public void endPrefixMapping( final String prefix ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.endPrefixMapping( prefix );
    }

    @Override
    public void startElement( final String uri, final String localName, final String qName, final Attributes atts ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.startElement( uri, localName, qName, atts );
    }

    @Override
    public void endElement( final String uri, final String localName, final String qName ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.endElement( uri, localName, qName );
    }

    @Override
    public void characters( final char[] ch, final int start, final int length ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.characters( ch, start, length );
    }

    @Override
    public void ignorableWhitespace( final char[] ch, final int start, final int length ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.ignorableWhitespace( ch, start, length );
    }

    @Override
    public void processingInstruction( final String target, final String data ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.processingInstruction( target, data );
    }

    @Override
    public void skippedEntity( final String name ) throws SAXException {
      for ( final ContentHandler handler : handlers )
        handler.skippedEntity( name );
    }
  }

  public static void main( final String[] args ) {
    /*
    try {
      final XHTMLParser parser = new XHTMLParser("");
      parser.init( new URI("http://toto.com") );
      final String fileName = args[0];
      try ( final java.io.InputStream is = new java.io.FileInputStream(fileName) ) {
        final HtmlDigestContentHandler digestContentHandler = new HtmlDigestContentHandler( parser.digestAppendable );
        final HtmlPureTextContentHandler pureTextContentHandler = new HtmlPureTextContentHandler( parser.pureTextAppendable );
        final HtmlBoilerpipeHandler boilerpipeHandler = new HtmlBoilerpipeHandler( ArticleExtractor.INSTANCE, MAX_BODY_LENGTH );
        final ContentHandler htmlContentHandler = new HtmlTeeContentHandler( digestContentHandler, pureTextContentHandler, boilerpipeHandler );
        final LinksHandler linksHandler = new LinksHandler( parser.pageInfo.getLinks(), MAX_ANCHOR_TEXT_LENGTH );
        final XhtmlContentHandler xhtmlContentHandler = new XhtmlContentHandler( parser.metadata, htmlContentHandler, linksHandler );
        final JerichoToXhtml jerichoToXhtml = new JerichoToXhtml( xhtmlContentHandler );
        final JerichoHtmlBackup jerichoHtmlBackup = new JerichoHtmlBackup( parser.rewritten );

        final StreamedSource streamedSource = new StreamedSource(new InputStreamReader( is, parser.pageInfo.getGuessedCharset() ));
        streamedSource.setBuffer( parser.buffer );
        final JerichoLoggerWrapper loggerWrapper = new JerichoLoggerWrapper( streamedSource.getLogger() );
        streamedSource.setLogger( loggerWrapper );
        final JerichoParser jerichoParser = new JerichoParser( streamedSource, new JerichoParser.TeeHandler(jerichoToXhtml,jerichoHtmlBackup) );

        jerichoParser.parse();

        System.out.println( "--------------------------" );
        System.out.println( parser.pureTextAppendable.getContent() );
        System.out.println( "--------------------------" );
        System.out.println( boilerpipeHandler.getContent() );
        System.out.println( "--------------------------" );
        System.out.println( "#error            : " + loggerWrapper.getErrorCount() );
        System.out.println( "#tagRejected      : " + loggerWrapper.getTagRejected() );
        System.out.println( "#tagNotRegistered : " + loggerWrapper.getTagNotRegistered() );
        System.out.println( "#otherError       : " + (loggerWrapper.getErrorCount() - loggerWrapper.getTagRejected() - loggerWrapper.getTagNotRegistered()) );
        System.out.println( "#warn             : " + loggerWrapper.getWarnCount() );
      }
    }
    catch ( java.io.IOException|JerichoParser.ParseException|java.net.URISyntaxException e ) {
      LOGGER.warn( "Failed to parse HTML", e );
    }
    */

    testHref( "https://stallman.org/glossay.html", "" );
    testHref( "https://stallman.org/glossay.html", "#" );
    testHref( "https://stallman.org/glossay.html", "javascript:;" );
    testHref( "https://stallman.org/glossay.html", "//:0" );
  }

  private static void testHref( final String source, final String link ) {
    try {
      System.out.println( "====================================================" );
      System.out.println( "source: '" + source + "', link: '" + link + "'" );
      System.out.println( "====================================================" );
      //final URI base = BURL.parse( source );
      //final URI href = BURL.parse( link );
      final URI base = new URI( source );
      final URI href = new URI( link );
      final URI target = href == null ? null : base.resolve( href );

      logUriInfo( "base", base );
      logUriInfo( "href", href );
      logUriInfo( "target", target );

      System.out.println( "base: " + toString(base) );
      System.out.println( "href: " + toString(href) );
      System.out.println( "target: " + toString(target) );
    }
    catch ( java.net.URISyntaxException exn ) {
      System.err.println( "Exception: " + exn.getMessage() );
    }
    finally {

    }
  }

  private static void logUriInfo( final String context, final URI uri ) {
    System.out.println( "-------------" + context + "-------------" );
    if ( uri == null )
      System.out.println( "<null>" );
    else {
      System.out.println( "'" + uri.toString() + "'" );
      System.out.println( "isAbsolute: " + uri.isAbsolute() );
      System.out.println( "isOpaque: " + uri.isOpaque() );

      System.out.println( "getScheme: " + uri.getScheme() );
      System.out.println( "getSchemeSpecificPart: " + uri.getSchemeSpecificPart() );
      System.out.println( "getAuthority: " + uri.getAuthority() );
      System.out.println( "getFragment: " + uri.getFragment() );
      System.out.println( "getHost: " + uri.getHost() );
      System.out.println( "getPath: " + uri.getPath() );
      System.out.println( "getQuery: " + uri.getQuery() );
    }
  }

  private static String toString( final URI uri ) {
    return uri == null ? "<null>" : uri.toString();
  }
}
