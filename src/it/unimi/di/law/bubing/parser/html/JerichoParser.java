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

import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.StartTagType;
import net.htmlparser.jericho.StreamedSource;


public final class JerichoParser
{
  static {
    /* As suggested by Martin Jericho. This should speed up things and avoid problems with
     * server tags embedded in weird places (e.g., JavaScript string literals). Server tags
     * should not appear in generated HTML anyway. */
    StartTagType.SERVER_COMMON.deregister();
    StartTagType.SERVER_COMMON_COMMENT.deregister();
    StartTagType.SERVER_COMMON_ESCAPED.deregister();
  }

  private final StreamedSource source;
  private final Handler handler;

  public JerichoParser( final StreamedSource source, final Handler handler ) {
    this.source = source;
    this.handler = handler;
  }

  public final void parse() throws ParseException {
    int lastSegmentEnd = 0;

    handler.startDocument( source );
    for ( final Segment segment : source ) {
      if ( segment.getEnd() <= lastSegmentEnd )
        continue;
      lastSegmentEnd = segment.getEnd();
      handler.process( source, segment );
    }
    handler.endDocument( source );
  }

  public static final class ParseException extends RuntimeException
  {
    public ParseException() {
      super();
    }

    public ParseException( final String message ) {
      super( message );
    }

    public ParseException( final Throwable cause ) {
      super( cause );
    }

    public ParseException( final String message, final Throwable cause ) {
      super( message, cause );
    }
  }

  public interface Handler
  {
    void startDocument( final StreamedSource source ) throws ParseException;
    void endDocument(  final StreamedSource source ) throws ParseException;
    void process(  final StreamedSource source, final Segment segment ) throws ParseException;
  }

  public static final class TeeHandler implements Handler
  {
    private final Handler[] handlers;

    public TeeHandler( final Handler... handlers ) {
      this.handlers = handlers;
    }

    @Override
    public final void startDocument( final StreamedSource source ) throws ParseException {
      for( final Handler handler : handlers )
        handler.startDocument( source );
    }

    @Override
    public final void endDocument( final StreamedSource source ) throws ParseException {
      for( final Handler handler : handlers )
        handler.endDocument( source );
    }

    @Override
    public final void process( final StreamedSource source, final Segment segment ) throws ParseException {
      for( final Handler handler : handlers )
        handler.process( source, segment );
    }
  }
}
