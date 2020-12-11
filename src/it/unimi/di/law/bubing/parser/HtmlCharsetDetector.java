package it.unimi.di.law.bubing.parser;

import com.ibm.icu.text.CharsetMatch;
import com.ibm.icu.text.ICUCharsetDetector;
import it.unimi.di.law.bubing.util.NoOpDecoder;
import net.htmlparser.jericho.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class HtmlCharsetDetector
{
  private static final int DEFAULT_ICU_BUFFER_SIZE = 5000;

  private final byte[] icuBuffer;
  private final ICUCharsetDetector charsetDetector;
  private int contentLength;

  public HtmlCharsetDetector( final int icuBufferSize ) {
    this.icuBuffer = new byte[ icuBufferSize ];
    this.charsetDetector = new ICUCharsetDetector();
    this.contentLength = 0;
  }

  public HtmlCharsetDetector() {
    this( DEFAULT_ICU_BUFFER_SIZE );
  }

  public String detect( final byte[] bytes, final int length, final char[] buffer ) throws IOException {
    fillIcuBuffer( bytes, length, buffer );
    final CharsetMatch match = charsetDetector.setText( icuBuffer, contentLength ).detect();
    return match == null ? null : match.getName();
  }

  public String detect( final byte[] bytes ) throws IOException {
    return detect( bytes, bytes.length, null );
  }

  public String detect( final byte[] bytes, final char[] buffer ) throws IOException {
    return detect( bytes, bytes.length, buffer );
  }

  public String detect( final byte[] bytes, final int length ) throws IOException {
    return detect( bytes, length, null );
  }

  private void fillIcuBuffer( final byte[] bytes, final int length, final char[] buffer )  throws IOException {
    final InputStream beginningOfStream = new ByteArrayInputStream( bytes, 0, length );
    int lastSegmentEnd = 0;
    int inSpecialText = 0;

    @SuppressWarnings("resource")
    final StreamedSource streamedSource = new StreamedSource(new InputStreamReader( beginningOfStream, new NoOpDecoder() ));

    if ( buffer != null )
      streamedSource.setBuffer( buffer );

    contentLength = 0;
    for ( final Segment segment : streamedSource ) {
      if ( segment.getEnd() <= lastSegmentEnd )
        continue;
      lastSegmentEnd = segment.getEnd();

      if ( segment instanceof StartTag ) {
        final StartTag startTag = (StartTag) segment;
        final StartTagType startTagType = startTag.getStartTagType();
        if ( startTagType == StartTagType.COMMENT )
          continue;
        final String name = startTag.getName();
        if ( (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) && !startTag.isSyntacticalEmptyElementTag() )
          inSpecialText++;
      }
      else
      if ( segment instanceof EndTag ) {
        final EndTag endTag = (EndTag) segment;
        final String name = endTag.getName();
        if ( name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT )
          inSpecialText = Math.max( 0, inSpecialText-1 ); // Ignore extra closing tags
      }
      else
      if ( inSpecialText == 0 && !(segment instanceof CharacterReference) ) {
        final java.nio.CharBuffer cb = streamedSource.getCurrentSegmentCharBuffer();
        contentLength = copyCharArrayToByteArray( cb.array(), cb.position(), cb.remaining(), icuBuffer, contentLength );
        if ( contentLength == icuBuffer.length )
          break;
      }
    }
  }

  private static int copyCharArrayToByteArray( final char[] in, final int inOff, final int inLen, final byte[] out, final int outOff ) {
    int i,o;
    for ( i=inOff, o=outOff; i<inOff+inLen && o<out.length; ++i, ++o )
      out[o] = (byte) in[i];
    return o;
  }

}
