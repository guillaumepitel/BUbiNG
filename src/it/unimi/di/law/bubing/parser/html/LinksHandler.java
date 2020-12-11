/*
 * Copyright (C) 2013-2023 eXenSa.
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

import it.unimi.di.law.bubing.parser.HTMLLink;
import net.htmlparser.jericho.HTMLElementName;
import org.xml.sax.Attributes;

import java.util.List;
import java.util.Stack;


public class LinksHandler
{
  private static final class Atts
  {
    static final String HREF  = "href";
    static final String SRC   = "src";
    static final String TITLE = "title";
    static final String REL   = "rel";
    static final String TYPE  = "type";
    static final String ALT   = "alt";
  }

  private static final class AnchorBuilder
  {
    private static final AnchorBuilder DUMMY = new AnchorBuilder();

    final String uri;
    final String title;
    final String rel;
    final StringBuilder text;
    final WhiteCharsCleaner whiteCharsCleaner;
    HTMLLink imgOpt;

    private AnchorBuilder() {
      this.uri = null;
      this.title = null;
      this.rel = null;
      this.text = new StringBuilder(0);
      this.whiteCharsCleaner = new WhiteCharsCleaner();
      this.imgOpt = null;
    }

    AnchorBuilder( final String uri, final String title, final String rel ) {
      this.uri = uri;
      this.title = title;
      this.rel = rel;
      this.text = new StringBuilder();
      this.whiteCharsCleaner = new WhiteCharsCleaner();
      this.imgOpt = null;
    }

    void characters( final char[] buffer, final int offset, final int length ) {
      final WhiteCharsCleaner.Iterator iterator = whiteCharsCleaner.getSegments( buffer, offset, length );
      while ( iterator.hasNext() ) {
        final WhiteCharsCleaner.Segment segment = iterator.next();
        text.append( segment.array, segment.offset, segment.length );
      }
    }

    void characters( final String text ) {
      final char[] chars = text.toCharArray();
      characters( chars, 0, chars.length );
    }

    String getAnchorText( final int maxAnchorTextLength ) {
      return text.length() > 0
        ? getAnchorText( text, maxAnchorTextLength )
        : null;
    }

    private static String getAnchorText( final StringBuilder sb, final int maxLength ) {
      return sb.length() > maxLength
        ? sb.substring( 0, maxLength )
        : sb.toString();
    }
  }

  private final List<HTMLLink> links;
  private final int maxAnchorTextLength;
  private final Stack<AnchorBuilder> anchors;
  private String baseOpt;

  public LinksHandler( final List<HTMLLink> links, final int maxAnchorTextLength ) {
    this.links = links;
    this.maxAnchorTextLength = maxAnchorTextLength;
    this.anchors = new Stack<>();
    this.baseOpt = null;
  }

  public void startTag( final String name, final Attributes attributes ) {
    if ( name == HTMLElementName.A ) startTagA( attributes );
    else if ( name == HTMLElementName.IMG ) startTagImg( attributes );
    else if ( name == HTMLElementName.LINK ) startTagLink( attributes );
    //else if ( name == HTMLElementName.SCRIPT ) startTagScript( attributes );
    else if ( name == HTMLElementName.EMBED ) startTagEmbed( attributes );
    else if ( name == HTMLElementName.IFRAME ) startTagIframe( attributes );
    else if ( name == HTMLElementName.FRAME ) startTagFrame( attributes );
    else if ( name == HTMLElementName.AREA ) startTagArea( attributes );
    else if ( name == HTMLElementName.BASE ) startTagBase( attributes );
  }

  public void endTag( final String name ) {
    if ( name == HTMLElementName.A ) endTagA();
  }

  public void characters( final char[] buffer, final int offset, final int length ) {
    if ( anchors.empty() ) return;
    final AnchorBuilder anchor = anchors.peek();
    if ( anchor != AnchorBuilder.DUMMY && anchor.text.length() < maxAnchorTextLength )
      anchor.characters( buffer, offset, length );
  }

  public void ignorableWhitespace( final char[] buffer, final int offset, final int length ) {
    characters( buffer, offset, length );
  }

  public String getBaseOpt() {
    return baseOpt;
  }

  public List<HTMLLink> getLinks() {
    return links;
  }

  private void startTagA( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.HREF );
    if ( uri == null )
      anchors.push( AnchorBuilder.DUMMY );
    else {
      final String title = attributes.getValue( Atts.TITLE );
      final String rel = attributes.getValue( Atts.REL );
      anchors.push( new AnchorBuilder(uri,title,rel) );
    }
  }

  private void endTagA() {
    if ( anchors.empty() ) return; // FIXME: should NOT happen
    final AnchorBuilder anchor = anchors.pop();
    if ( anchor == AnchorBuilder.DUMMY ) return;
    final String anchorText = anchor.getAnchorText( maxAnchorTextLength );
    if ( anchor.imgOpt == null )
      addLink( HTMLLink.Type.A, anchor.uri, anchor.title, anchorText, anchor.rel );
    else
    if ( anchorText != null && !anchorText.isBlank() )
      addLink( HTMLLink.Type.A, anchor.uri, anchor.title, anchorText, anchor.rel );
    else
      addLink( HTMLLink.Type.IMG, anchor.uri, anchor.title, anchor.imgOpt.text, anchor.rel );
  }

  private void startTagLink( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.HREF );
    if ( uri == null ) return;
    final String rel = attributes.getValue( Atts.REL );
    addLink( HTMLLink.Type.LINK, uri, null, null, rel );
  }

  private void startTagScript( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.SRC );
    if ( uri == null ) return;
    addLink( HTMLLink.Type.SCRIPT, uri, null, null, null );
  }

  private void startTagEmbed( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.SRC );
    if ( uri == null ) return;
    final String title = attributes.getValue( Atts.TITLE );
    final String rel = attributes.getValue( Atts.TYPE );
    addLink( HTMLLink.Type.EMBED, uri, title, null, rel );
  }

  private void startTagIframe( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.SRC );
    if ( uri == null ) return;
    final String title = attributes.getValue( Atts.TITLE );
    addLink( HTMLLink.Type.IFRAME, uri, title, null, null );
  }

  private void startTagFrame( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.SRC );
    if ( uri == null ) return;
    addLink( HTMLLink.Type.FRAME, uri, null, null, null );
  }

  private void startTagArea( final Attributes attributes ) {
    final String uri = attributes.getValue( Atts.HREF );
    if ( uri == null ) return;
    final String title = attributes.getValue( Atts.TITLE );
    final String text = attributes.getValue( Atts.ALT );
    final String rel = attributes.getValue( Atts.REL );
    addLink( HTMLLink.Type.IMG, uri, title, text, rel );
  }

  private void startTagImg( final Attributes attributes ) {
    if ( anchors.empty() ) return;
    final AnchorBuilder anchor = anchors.peek();
    if ( anchor == AnchorBuilder.DUMMY ) return;
    final String uri = attributes.getValue( Atts.SRC );
    if ( uri == null ) return;
    final String title = attributes.getValue( Atts.TITLE );
    final String alt = attributes.getValue( Atts.ALT );
    anchor.imgOpt = new HTMLLink( HTMLLink.Type.IMG, uri, title, alt, null );
  }

  private void startTagBase( final Attributes attributes ) {
    if ( baseOpt != null ) return;
    baseOpt = attributes.getValue( Atts.HREF );
  }

  private void addLink( final String type, final String uri, final String title, final String text, final String rel ) {
    links.add( new HTMLLink(type,uri,title,text,rel) );
  }
}
