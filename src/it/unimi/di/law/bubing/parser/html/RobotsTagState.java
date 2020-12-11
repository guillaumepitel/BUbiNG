package it.unimi.di.law.bubing.parser.html;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Locale;
import java.util.regex.Pattern;


public final class RobotsTagState
{
  private static final String NONE = "none";
  public static final String NOINDEX = "noindex";
  public static final String NOFOLLOW = "nofollow";
  public static final String NOARCHIVE = "noarchive";
  public static final String NOSNIPPET = "nosnippet";

  private static final Pattern SPLIT_PATTERN = Pattern.compile( "\\s+|,\\s*" );
  private static final ObjectOpenHashSet<String> ALL_TAGS = makeSet( NOINDEX, NOFOLLOW, NOARCHIVE, NOSNIPPET );

  private static ObjectOpenHashSet<String> makeSet( String... elements ) {
    return new ObjectOpenHashSet<>( elements );
  }

  public static RobotsTagState from( final String line ) {
    return new RobotsTagState().add( line );
  }

  private final ObjectOpenHashSet<String> state;

  public RobotsTagState() {
    this.state = new ObjectOpenHashSet<>( 4, 0.75f );
  }

  public RobotsTagState add( final RobotsTagState other ) {
    state.addAll( other.state );
    return this;
  }

  public RobotsTagState add( final String line ) {
    for ( final String s : SPLIT_PATTERN.split( line.trim().toLowerCase(Locale.ROOT) ) ) {
      if ( ALL_TAGS.contains(s) )
        state.add( s );
      else
      if ( s.equals(NONE) ) {
        state.add( NOINDEX );
        state.add( NOFOLLOW );
      }
    }
    return this;
  }

  public boolean contains( final String tag ) {
    return state.contains( tag );
  }
}
