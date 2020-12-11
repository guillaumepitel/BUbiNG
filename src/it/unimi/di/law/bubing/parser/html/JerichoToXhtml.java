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

import net.htmlparser.jericho.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Stack;

import static net.htmlparser.jericho.HTMLElementName.*;


public class JerichoToXhtml implements JerichoParser.Handler
{
  private static final Logger LOGGER = LoggerFactory.getLogger( JerichoToXhtml.class );
  private static final AttributesImpl emptyAttributes = new AttributesImpl();
  private static final String XHTML_URL = "http://www.w3.org/1999/xhtml";
  private final ContentHandler handler;
  private final DecodedCharacterReference characterReference;
  private final XhtmlHelper helper;
  private final Stack<String> stack;
  private boolean firstErr = true; // for debug

  public JerichoToXhtml( final ContentHandler handler ) {
    this.handler = handler;
    this.characterReference = new DecodedCharacterReference();
    this.helper = new XhtmlHelper( this );
    this.stack = new Stack<>();
  }

  @Override
  public void startDocument( final StreamedSource source ) throws JerichoParser.ParseException {
    try {
      handler.startDocument();
    }
    catch ( SAXException e ) {
      throw new JerichoParser.ParseException( e );
    }
  }

  @Override
  public void endDocument( final StreamedSource source ) throws JerichoParser.ParseException {
    try {
      if ( LOGGER.isDebugEnabled() )
        closeAllPrevWithLog( "endDocument" );
      else
        closeAllPrevNoLog();
      handler.endDocument();
    }
    catch ( SAXException e ) {
      throw new JerichoParser.ParseException( e );
    }
  }

  @Override
  public void process( final StreamedSource source, final Segment segment ) throws JerichoParser.ParseException {
    try {
      if ( segment instanceof StartTag ) {
        final StartTag startTag = (StartTag) segment;
        if ( startTag.getStartTagType() == StartTagType.NORMAL )
          startTag( startTag );
      }
      else
      if ( segment instanceof EndTag ) {
        final EndTag endTag = (EndTag) segment;
        if ( endTag.getEndTagType() == EndTagType.NORMAL )
          endTag( endTag );
      }
      else
      if ( segment instanceof CharacterReference ) {
        characterReference.reset();
        final CharacterReference charRef = (CharacterReference) segment;
        charRef.appendCharTo( characterReference );
        characters( characterReference.array(), 0, characterReference.length() );
      }
      else {
        final java.nio.CharBuffer buffer = source.getCurrentSegmentCharBuffer();
        characters( buffer.array(), buffer.position(), buffer.remaining() );
      }
    }
    catch ( SAXException | IOException e ) {
      throw new JerichoParser.ParseException( e );
    }
  }

  private void startTag( final StartTag startTag ) throws SAXException {
    final String name = startTag.getName();
    closePrevFromStartTagIfNeeded( name ); // FIXME: does not call helper.endElement
    helper.startElement( XHTML_URL, name, name, toXmlSaxAttributes(startTag.getAttributes()) );
    if ( startTag.isSyntacticalEmptyElementTag() || startTag.isEndTagForbidden() )
      helper.endElement( XHTML_URL, name, name );
    //else
    //  stack.push( startTag );
  }

  private void endTag( final EndTag endTag ) throws SAXException {
    final String name = endTag.getName();
    //closePrevFromEndTagIfNeeded( name );
    helper.endElement( XHTML_URL, name, name ); // FIXME: do we need to close prev before ?
    //stack.pop();
  }

  private void characters( final char[] buffer, final int offset, final int length ) throws SAXException {
    handler.characters( buffer, offset, length );
  }

  private void closePrevFromStartTagIfNeeded( final String curr ) throws SAXException {
    if ( LOGGER.isDebugEnabled() )
      closePrevFromStartTagIfNeededWithLog( curr );
    else
      closePrevFromStartTagIfNeededNoLog( curr );
  }

  private void closePrevFromStartTagIfNeededWithLog( final String curr ) throws SAXException {
    final String toClose = matchStartTagInStack( curr );
    if ( toClose != null )
      closePrevWithLog( "startTag", curr, toClose );
  }

  private void closePrevFromStartTagIfNeededNoLog( final String curr ) throws SAXException {
    final String toClose = matchStartTagInStack( curr );
    if ( toClose != null )
      closePrevNoLog( toClose );
  }

  private String matchStartTagInStack( final String curr ) {
    String match = null;
    for ( int i=stack.size()-1; i>=0; --i ) {
      final String prev = stack.elementAt(i);
      if ( needsClose(prev,curr) )
        match = prev;
      if ( nonTerminating(prev,curr) )
        return match;
    }
    return match;
  }

  private static boolean nonTerminating( final String prev, final String curr ) {
    final java.util.Set<String> nonterminatingElementNames = HTMLElements.getNonterminatingElementNames( prev );
    return nonterminatingElementNames == null || nonterminatingElementNames.contains( curr );
  }

  private static boolean needsClose( final String prev, final String curr ) {
    final java.util.Set<String> terminatingStartTagNames = HTMLElements.getTerminatingStartTagNames( prev );
    return terminatingStartTagNames != null && terminatingStartTagNames.contains( curr );
  }

  private void closePrevFromEndTagIfNeeded( final String curr ) throws SAXException {
    if ( LOGGER.isDebugEnabled() )
      closePrevFromEndTagIfNeededWithLog( curr );
    else
      closePrevFromEndTagIfNeededNoLog( curr );
  }

  private void closePrevFromEndTagIfNeededNoLog( final String curr ) throws SAXException {
    final String toClose = matchEndTagInStack( curr );
    if ( toClose != null )
      closePrevNoLog( toClose );
  }

  private void closePrevFromEndTagIfNeededWithLog( final String curr ) throws SAXException {
    final String toClose = matchEndTagInStack( curr );
    if ( toClose == null )
      LOGGER.debug( String.format("endTag [%s] skipped",curr) );
    else
      closePrevWithLog( "endTag", curr, toClose );
  }

  private String matchEndTagInStack( final String curr ) {
    for ( int i=stack.size()-1; i>=0; --i ) {
      final String prev = stack.elementAt(i);
      if ( prev.equals(curr) ) // using equals for non-standard HTML tags
        return prev;
    }
    return null;
  }

  private void closePrevNoLog( final String toClose ) throws SAXException {
    String prev;
    do {
      prev = stack.pop();
      handler.endElement( XHTML_URL, prev, prev );
    } while ( prev != toClose );
  }

  private void closePrevWithLog( final String from, final String curr, final String toClose ) throws SAXException {
    String prev;
    int i = 1;
    do {
      prev = stack.pop();
      if ( prev != toClose )
        LOGGER.debug( String.format("%s [%s] force close of [%s] (%d)",from,curr,prev,i) );
      handler.endElement( XHTML_URL, prev, prev );
      i += 1;
    } while ( prev != toClose );
  }

  private void closeAllPrevWithLog( final String reason ) throws SAXException {
    for ( int i=stack.size(); i>0; --i ) {
      final String prev = stack.pop();
      LOGGER.debug( String.format("%s force close of [%s] (%d)",reason,prev,i) );
      handler.endElement( XHTML_URL, prev, prev );
    }
  }

  private void closeAllPrevNoLog() throws SAXException {
    for ( int i=stack.size(); i>0; --i ) {
      final String prev = stack.pop();
      handler.endElement( XHTML_URL, prev, prev );
    }
  }

  private void printErr( final String msg ) {
    if ( firstErr ) {
      System.err.println( "------------------------------------------------------------" );
      firstErr = false;
    }
    System.err.println( msg );
  }

  private void startElement( final String uri, final String localName, final String qName, final Attributes atts ) throws SAXException {
    stack.push( localName );
    handler.startElement( uri, localName, qName, atts );
  }

  private void endElement( final String uri, final String localName, final String qName ) throws SAXException {
    closePrevFromEndTagIfNeeded( localName );
  }

  private void ignorableWhitespace( final char[] ch, final int start, final int length ) throws SAXException {
    handler.ignorableWhitespace( ch, start, length );
  }

  private org.xml.sax.Attributes toXmlSaxAttributes( final net.htmlparser.jericho.Attributes attributes ) {
    if ( attributes == null )
      return emptyAttributes;
    return new org.xml.sax.Attributes() {
      @Override
      public final int getLength() {
        return attributes.getCount();
      }

      @Override
      public final String getURI( final int index ) {
        if ( index < 0 || index >= attributes.length() )
          return null;
        return "";
      }

      @Override
      public final String getLocalName( final int index ) {
        if ( index < 0 || index >= attributes.length() )
          return null;
        return attributes.get(index).getKey();
      }

      @Override
      public final String getQName( final int index ) {
        return getLocalName( index );
      }

      @Override
      public final String getType( final int index ) {
        if ( index < 0 || index >= attributes.length() )
          return null;
        return "CDATA";
      }

      @Override
      public final String getValue( final int index ) {
        if ( index < 0 || index >= attributes.length() )
          return null;
        final String value = attributes.get(index).getValue();
        return value == null ? "" : value;
      }

      @Override
      public final int getIndex( final String uri, final String localName ) {
        int i=0;
        for ( final net.htmlparser.jericho.Attribute attribute : attributes ) {
          if ( attribute.getName().equalsIgnoreCase(localName) )
            return i;
          i += 1;
        }
        return -1;
      }

      @Override
      public final int getIndex( final String qName ) {
        return getIndex( "", qName );
      }

      @Override
      public final String getType( final String uri, final String localName ) {
        return getType( getIndex(uri,localName) );
      }

      @Override
      public final String getType( final String qName ) {
        return getType( "", qName );
      }

      @Override
      public final String getValue( final String uri, final String localName ) {
        return getValue( getIndex(uri,localName) );
      }

      @Override
      public final String getValue( final String qName ) {
        return getValue( "", qName );
      }
    };
  }

  private static final class DecodedCharacterReference implements Appendable
  {
    private final char[] _chars;
    private int _length;

    DecodedCharacterReference() {
      this._chars = new char[2];
      this._length = 0;
    }

    @Override
    public final Appendable append( CharSequence csq ) throws IOException {
      throw new IOException( "CharSequence is not supported" );
    }

    @Override
    public final Appendable append( CharSequence csq, int start, int end ) throws IOException {
      throw new IOException( "CharSequence is not supported" );
    }

    @Override
    public final Appendable append( char c ) throws IOException {
      _chars[ _length++ ] = c;
      return this;
    }

    final char[] array() {
      return _chars;
    }

    final int length() {
      return _length;
    }

    final void reset() {
      _length = 0;
    }
  }

  private static final class XhtmlHelper
  {
    private static final char[] NEWLINE = new char[] { '\n' };
    private static final char[] TAB = new char[] { '\t' };

    private static final HashSet<String> ONLY_IN_HEAD_SET = makeSet( TITLE, LINK, META, BASE );
    private static final HashSet<String> ALLOWED_IN_HEAD_SET = makeSet( TITLE, LINK, META, BASE, STYLE, SCRIPT, NOSCRIPT );
    private static final HashSet<String> AUTO_SET = makeSet( HTML, HEAD, BODY );
    private static final HashSet<String> ENDLINE_SET = makeSet( ARTICLE,ASIDE,FOOTER,DETAILS,SECTION,HEADER,HGROUP,NAV,P,H1,H2,H3,H4,H5,H6,UL,OL,DIR,MENU,PRE,DL,DIV,CENTER,NOSCRIPT,NOFRAMES,BLOCKQUOTE,FORM,ISINDEX,HR,TABLE,FIELDSET,ADDRESS,LI,DT,DD,TR,CAPTION,LEGEND,BR );
    private static final HashSet<String> INDENT_SET = makeSet( LI, DD );

    private static final class State
    {
      static final int UNDEF = 0;
      static final int IN_HTML = 1;
      static final int IN_HEAD = 2;
      static final int IN_HEADER = 3;
      static final int IN_BODY = 4;
      static final int IN_FOOTER = 5;
    }

    private static HashSet<String> makeSet( String... elements ) {
      return new HashSet<>( Arrays.asList(elements) );
    }

    private final JerichoToXhtml xhtml;
    private int state;
    // I found HTML tags in <STYLE> section and some of those tags were forbidden in <HEAD> section
    // XhtmlHelper was closing <HEAD> to open <BODY> in the middle of the <STYLE> section which leaves the
    // end of the <STYLE> section as plain text (i.e. characters)
    // So I decided to allow any tags in a <STYLE> section within a <HEAD> section
    private int inStyle;

    XhtmlHelper( final JerichoToXhtml xhtml) {
      this.state = State.UNDEF;
      this.xhtml = xhtml;
      this.inStyle = 0;
    }

    /*
    void startDocument() throws SAXException {
      xhtml.startDocument();
    }

    void endDocument() throws SAXException {
      lazyEndHead( "endDocument" );
      autoClose( BODY, "endDocument" );
      autoClose( HTML, "endDocument" );
      xhtml.endDocument();
    }
    */

    void startElement( final String uri, final String localName, final String qName, final Attributes atts ) throws SAXException {
      if ( state >= State.IN_BODY ) {
        if ( JerichoToXhtml.LOGGER.isDebugEnabled() && ONLY_IN_HEAD_SET.contains(localName) )
          JerichoToXhtml.LOGGER.debug( String.format("[%s] should be in <head>",localName) );
        startElementImpl( uri, localName, qName, atts );
      }
      else
        startElementSlowPath( uri, localName, qName, atts );
    }

    void endElement( final String uri, final String localName, final String qName ) throws SAXException {
      endElementImpl( uri, localName, qName );
    }

    private void startElementSlowPath( final String uri, final String localName, final String qName, final Attributes atts ) throws SAXException {
      if ( inStyle == 0 && !AUTO_SET.contains(localName) ) {
        if ( ONLY_IN_HEAD_SET.contains(localName) )
          lazyStartHead( localName );
        else
        if ( !ALLOWED_IN_HEAD_SET.contains(localName) )
          lazyEndHead( localName );
      }
      startElementImpl( uri, localName, qName, atts );
    }

    private void lazyStartHead( final String reason ) throws SAXException {
      if ( state < State.IN_HEAD ) {
        autoOpen( HTML, reason );
        autoOpen( HEAD, reason );
      }
    }

    private void lazyEndHead( final String reason ) throws SAXException {
      lazyStartHead( reason );
      if ( state < State.IN_BODY ) {
        autoClose( HEAD, reason );
        autoOpen( BODY, reason );
      }
    }

    private void autoOpen( final String tag, final String reason ) throws SAXException {
      if ( JerichoToXhtml.LOGGER.isDebugEnabled() )
        JerichoToXhtml.LOGGER.debug( String.format("[%s] force open [%s]",reason,tag) );
      startElementImpl( XHTML_URL, tag, tag, JerichoToXhtml.emptyAttributes );
    }

    private void autoClose( final String tag, final String reason ) throws SAXException {
      if ( JerichoToXhtml.LOGGER.isDebugEnabled() )
        JerichoToXhtml.LOGGER.debug( String.format("[%s] force close [%s]",reason,tag) );
      endElementImpl( XHTML_URL, tag, tag );
    }

    private void startElementImpl( final String uri, final String localName, final String qName, final Attributes atts ) throws SAXException {
      if ( state < State.IN_HTML && localName == HTML )
        setState( State.IN_HTML );
      else
      if ( state < State.IN_HEAD && localName == HEAD )
        setState( State.IN_HEAD );
      else
      if ( state < State.IN_BODY && localName == BODY )
        setState( State.IN_BODY );

      if ( ENDLINE_SET.contains(localName) )
        newline();
      if ( INDENT_SET.contains(localName) )
        indent();

      if ( localName == STYLE )
        inStyle += 1;
      xhtml.startElement( uri, localName, qName, atts );
    }

    private void endElementImpl( final String uri, final String localName, final String qName ) throws SAXException {
      if ( localName == HTML && ( state == State.IN_HTML || state == State.IN_HEADER || state == State.IN_FOOTER )  )
        setState( State.UNDEF );
      else
      if ( localName == HEAD && state == State.IN_HEAD  )
        setState( State.IN_HEADER );
      else
      if ( localName == BODY && state == State.IN_BODY )
        setState( State.IN_FOOTER );

      if ( ENDLINE_SET.contains(localName) )
        newline();

      if ( localName == STYLE && inStyle > 0 )
        inStyle -= 1;
      xhtml.endElement( uri, localName, qName );
    }

    private void newline() throws SAXException {
      xhtml.ignorableWhitespace( NEWLINE, 0, NEWLINE.length );
    }

    private void indent() throws SAXException {
      xhtml.ignorableWhitespace( TAB, 0, TAB.length );
    }

    private static String stateToString( final int state ) {
      switch ( state ) {
        case State.UNDEF: return "UNDEF";
        case State.IN_HTML: return "IN_HTML";
        case State.IN_HEAD: return "IN_HEAD";
        case State.IN_HEADER: return "IN_HEADER";
        case State.IN_BODY: return "IN_BODY";
        case State.IN_FOOTER: return "IN_FOOTER";
        default: return "## ERROR ##";
      }
    }

    private void setState( final int state ) {
      //xhtml.printErr( String.format("%s -> %s",stateToString(this.state),stateToString(state)) );
      this.state = state;
    }
  }
}
