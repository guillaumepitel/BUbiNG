/*
 * Copyright (c) 2013-2023 eXenSa.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.unimi.di.law.bubing.parser.html;

import it.unimi.di.law.bubing.parser.Metadata;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.util.Locale;

import static net.htmlparser.jericho.HTMLElementName.*;


public class XhtmlContentHandler implements ContentHandler
{
  private final ContentHandler handler;
  private final Metadata metadata;
  private final LinksHandler linksHandlerOpt;
  private final StringBuilder title;
  private int skipLevel;
  private int titleLevel;
  private int bodyLevel;

  public XhtmlContentHandler( final Metadata metadata, final ContentHandler handler, final LinksHandler linksHandlerOpt ) {
    this.metadata = metadata;
    this.handler = handler;
    this.linksHandlerOpt = linksHandlerOpt;
    this.title = new StringBuilder();
    this.skipLevel = 0;
    this.titleLevel = 0;
    this.bodyLevel = 0;
  }

  public XhtmlContentHandler( final ContentHandler handler, final LinksHandler linksHandlerOpt ) {
    this( new Metadata(), handler, linksHandlerOpt );
  }

  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public void setDocumentLocator( Locator locator ) {
    handler.setDocumentLocator( locator );
  }

  @Override
  public void startDocument() throws SAXException {
    handler.startDocument();
  }

  @Override
  public void endDocument() throws SAXException {
    handler.endDocument();
  }

  @Override
  public void startPrefixMapping( String prefix, String uri ) throws SAXException {
    handler.startPrefixMapping( prefix, uri );
  }

  @Override
  public void endPrefixMapping( String prefix ) throws SAXException {
    handler.endPrefixMapping( prefix );
  }

  @Override
  public void startElement( String uri, String localName, String qName, Attributes atts ) throws SAXException {
    if ( localName == SCRIPT || localName == STYLE )
      skipLevel += 1;
    if ( skipLevel > 0 )
      return;

    if ( localName == TITLE || titleLevel > 0 ) {
      titleLevel += 1;
      title.setLength( 0 );
    }
    else
    if ( localName == BODY /*|| tagName == FRAMESET*/ || bodyLevel > 0 )
      bodyLevel += 1;

    if ( localName == META && bodyLevel == 0 )
      startTagMeta( localName, atts );

    if ( linksHandlerOpt != null )
      linksHandlerOpt.startTag( localName, atts );
    handler.startElement( uri, localName, qName, atts );
  }

  @Override
  public void endElement( String uri, String localName, String qName ) throws SAXException {
    if ( localName == SCRIPT || localName == STYLE ) {
      skipLevel = Math.max( 0, skipLevel-1 );
      return;
    }
    else
    if ( skipLevel > 0 )
      return;
    else
    if ( bodyLevel > 0 )
      bodyLevel -= 1;

    if ( linksHandlerOpt != null )
      linksHandlerOpt.endTag( localName );

    handler.endElement( uri, localName, qName );

    if ( titleLevel > 0 ) {
      titleLevel -= 1;
      if ( titleLevel == 0 && metadata.get("title") == null )
        metadata.set( "title", title.toString().trim() );
    }
  }

  @Override
  public void characters( char[] ch, int start, int length ) throws SAXException {
    if ( skipLevel > 0 )
      return;

    if ( titleLevel > 0 && bodyLevel == 0 )
      title.append( ch, start, length );

    if ( linksHandlerOpt != null )
      linksHandlerOpt.characters( ch, start, length );

    handler.characters( ch, start, length );
  }

  @Override
  public void ignorableWhitespace( char[] ch, int start, int length ) throws SAXException {
    if ( skipLevel > 0 )
      return;

    if ( linksHandlerOpt != null )
      linksHandlerOpt.ignorableWhitespace( ch, start, length );

    handler.ignorableWhitespace( ch, start, length );
  }

  @Override
  public void processingInstruction( String target, String data ) throws SAXException {
    handler.processingInstruction( target, data );
  }

  @Override
  public void skippedEntity( String name ) throws SAXException {
    handler.skippedEntity( name );
  }

  private void startTagMeta( final String name, final Attributes attributes ) {
    boolean dummy =
      tryMetaWithContent( attributes, attributes.getValue("content") ) ||
      tryMetaCharset( attributes );
  }

  private boolean tryMetaWithContent( final Attributes attributes, final String content ) {
    return content != null && (
      tryAddMetadata( attributes, "http-equiv", content ) ||
      tryAddMetadata( attributes, "name", content ) ||
      tryAddMetadata( attributes, "property", content )
    );
  }

  private boolean tryMetaCharset( final Attributes attributes ) {
    final String charset = attributes.getValue( "charset" );
    if ( charset == null ) return false;
    metadata.add( "charset", charset );
    return true;
  }

  private boolean tryAddMetadata( final Attributes attributes, final String name, final String content ) {
    final String key = attributes.getValue( name );
    if ( key == null ) return false;
    metadata.add( key.toLowerCase(Locale.ROOT), content );
    return true;
  }
}
