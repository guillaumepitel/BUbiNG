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

import com.kohlschutter.boilerpipe.BoilerpipeExtractor;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.document.TextBlock;
import com.kohlschutter.boilerpipe.document.TextDocument;
import com.kohlschutter.boilerpipe.sax.BoilerpipeHTMLContentHandler;
import net.htmlparser.jericho.HTMLElementName;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;


public final class HtmlBoilerpipeHandler extends BoilerpipeHTMLContentHandler
{
  public static final class WriteLimitReachedException extends SAXException
  {
    WriteLimitReachedException( final int capacity ) {
      super( String.format("The write limit capacity of %d has been reached",capacity) );
    }
  }

  private final BoilerpipeExtractor extractor;
  private final int capacity;
  private final StringBuilder stringBuilder;
  private boolean inAnchor;

  public HtmlBoilerpipeHandler( final BoilerpipeExtractor extractor, final int capacity ) {
    this.extractor = extractor;
    this.capacity = capacity;
    this.stringBuilder = new StringBuilder( capacity );
    this.inAnchor = false;
  }

  public StringBuilder getContent() {
    return stringBuilder;
  }

  @Override
  public void startElement( String uri, String localName, String name, Attributes atts ) throws SAXException {
    if ( localName == HTMLElementName.A ) {
      if ( inAnchor )
        super.endElement( uri, localName, name );
      inAnchor = true;
    }
    super.startElement( uri, localName, name, atts );
  }

  @Override
  public void endElement( String uri, String localName, String name ) throws SAXException {
    if ( localName == HTMLElementName.A ) {
      if ( !inAnchor ) return;
      inAnchor = false;
    }
    super.endElement( uri, localName, name );
  }

  @Override
  public void endDocument() throws SAXException {
    super.endDocument();

    final TextDocument document = toTextDocument();

    try {
      extractor.process( document );
    }
    catch ( BoilerpipeProcessingException e ) {
      throw new SAXException(e);
    }

    for ( TextBlock block : document.getTextBlocks() ) {
      if ( stringBuilder.length() >= capacity )
        break;
      if ( block.isContent() ) {
        final String text = block.getText();
        if ( text.length() > 0 ) {
          if ( stringBuilder.length()+text.length()+1 > capacity )
            stringBuilder.append( text.substring(0,capacity-stringBuilder.length()) );
          else {
            stringBuilder.append( text );
            stringBuilder.append( '\n' );
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return stringBuilder.toString();
  }
}
