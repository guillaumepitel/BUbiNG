package it.unimi.di.law.bubing.parser;

import com.exensa.wdl.protobuf.link.EnumRel;
import com.exensa.wdl.protobuf.link.EnumType;
import com.exensa.wdl.protobuf.link.MsgLink;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;


public final class LinksHelper
{
  private static final Logger LOGGER = LoggerFactory.getLogger( LinksHelper.class );

  static Iterable<HTMLLink> fromHttpHeader( final String header ) {
    return HttpLinksHeaderParser.tryParse( header );
  }

  public static boolean trySetLinkInfos( final HTMLLink link, final MsgLink.LinkInfo.Builder linkInfoBuilder, final int linkNum, final boolean noFollow ) {
    final String type = link.type;
    if ( type == HTMLLink.Type.A && processRels(linkInfoBuilder,link.rel,allowedRelsMap_Anchors) )
      linkInfoBuilder.setLinkTypeValue( EnumType.Enum.A_VALUE );
    else if ( type == HTMLLink.Type.IMG && processRels(linkInfoBuilder,link.rel,allowedRelsMap_Anchors) )
      linkInfoBuilder.setLinkTypeValue( EnumType.Enum.IMG_VALUE );
    else if ( type == HTMLLink.Type.LINK && processRels(linkInfoBuilder,link.rel,allowedRelsMap_Links) )
      linkInfoBuilder.setLinkTypeValue( EnumType.Enum.LINK_VALUE );
    else if ( type == HTMLLink.Type.REDIRECT )
      linkInfoBuilder.setLinkTypeValue( EnumType.Enum.REDIRECT_VALUE );
    else
      return false; // FIXME: exclude all other link types

    if ( noFollow )
      linkInfoBuilder.setLinkRel( linkInfoBuilder.getLinkRel() | EnumRel.Enum.NOFOLLOW_VALUE );

    if ( link.text != null )
      linkInfoBuilder.setText( link.text.toLowerCase(Locale.ROOT).trim() );
    else
      linkInfoBuilder.clearText();
    linkInfoBuilder.setLinkQuality( (float) ( 1. - ( 1. / (1. + Math.exp(-(linkNum - 150.) / 30.)))));
    return true;
  }

  private static boolean processRels( final MsgLink.LinkInfo.Builder linkInfoBuilder, final String rel, final Object2IntOpenHashMap<String> map ) {
    int relValue = 0;
    if ( rel != null ) {
      final String[] rels = SPLIT_PATTERN.split( rel.toLowerCase(Locale.ROOT).trim() );
      for ( final String r : rels ) {
        if ( excludeRels.contains(r) )
          return false;
        relValue |= map.getInt(r);
      }
    }
    linkInfoBuilder.setLinkRel( relValue );
    return true;
  }

  private static final Pattern SPLIT_PATTERN = Pattern.compile( "\\s+|,\\s*" );

  private static final Set<String> excludeRels = makeSet(
    "stylesheet",
    "prefetch",
    "dns-prefetch",
    "preconnect",
    "preload",
    "prerender",
    "shortcut",
    "icon",
    "mask-icon",
    "meta",
    "apple-touch-icon",
    "apple-touch-icon-precomposed",
    "apple-touch-startup-image",
    "image_src",
    "edituri",
    "https://api.w.org/",
    "manifest",
    "wlwmanifest",
    "profile",
    "publisher",
    "enclosure",
    "pingback"
  );

  private static final Object2IntOpenHashMap<String> allowedRelsMap_Links = makeMap(
    E( "canonical", EnumRel.Enum.CANONICAL_VALUE ),
    E( "shortlink", EnumRel.Enum.SHORTLINK_VALUE ),
    E( "index",     EnumRel.Enum.INDEX_VALUE ),
    E( "search",    EnumRel.Enum.SEARCH_VALUE ),
    E( "alternate", EnumRel.Enum.ALTERNATE_VALUE ),
    E( "start",     EnumRel.Enum.FIRST_VALUE ),
    E( "first",     EnumRel.Enum.FIRST_VALUE ),
    E( "begin",     EnumRel.Enum.FIRST_VALUE ),
    E( "prev",      EnumRel.Enum.PREV_VALUE ),
    E( "previous",  EnumRel.Enum.PREV_VALUE ),
    E( "next",      EnumRel.Enum.NEXT_VALUE ),
    E( "last",      EnumRel.Enum.LAST_VALUE ),
    E( "end",       EnumRel.Enum.LAST_VALUE ),
    E( "home",      EnumRel.Enum.HOME_VALUE ),
    E( "author",    EnumRel.Enum.AUTHOR_VALUE ),
    E( "license",   EnumRel.Enum.LICENSE_VALUE ),
    E( "archives",  EnumRel.Enum.ARCHIVES_VALUE ),
    E( "archive",   EnumRel.Enum.ARCHIVES_VALUE ),
    E( "ugc",       EnumRel.Enum.UGC_VALUE ),
    E( "sponsored", EnumRel.Enum.SPONSORED_VALUE )
  );

  private static final Object2IntOpenHashMap<String> allowedRelsMap_Anchors = makeMap(
    E( "nofollow",   EnumRel.Enum.NOFOLLOW_VALUE ),
    E( "noopener",   EnumRel.Enum.NOOPENER_VALUE ),
    E( "noreferrer", EnumRel.Enum.NOREFERRER_VALUE ),
    E( "canonical",  EnumRel.Enum.CANONICAL_VALUE ),
    E( "bookmark",   EnumRel.Enum.BOOKMARK_VALUE ),
    E( "shortlink",  EnumRel.Enum.SHORTLINK_VALUE ),
    E( "tag",        EnumRel.Enum.TAG_VALUE ),
    E( "category",   EnumRel.Enum.TAG_VALUE ),
    E( "index",      EnumRel.Enum.INDEX_VALUE ),
    E( "search",     EnumRel.Enum.SEARCH_VALUE ),
    E( "alternate",  EnumRel.Enum.ALTERNATE_VALUE ),
    E( "start",      EnumRel.Enum.FIRST_VALUE ),
    E( "first",      EnumRel.Enum.FIRST_VALUE ),
    E( "begin",      EnumRel.Enum.FIRST_VALUE ),
    E( "prev",       EnumRel.Enum.PREV_VALUE ),
    E( "previous",   EnumRel.Enum.PREV_VALUE ),
    E( "next",       EnumRel.Enum.NEXT_VALUE ),
    E( "last",       EnumRel.Enum.LAST_VALUE ),
    E( "end",        EnumRel.Enum.LAST_VALUE ),
    E( "home",       EnumRel.Enum.HOME_VALUE ),
    E( "author",     EnumRel.Enum.AUTHOR_VALUE ),
    E( "license",    EnumRel.Enum.LICENSE_VALUE ),
    E( "archives",   EnumRel.Enum.ARCHIVES_VALUE ),
    E( "archive",    EnumRel.Enum.ARCHIVES_VALUE ),
    E( "external",   EnumRel.Enum.EXTERNAL_VALUE ),
    E( "ugc",        EnumRel.Enum.UGC_VALUE ),
    E( "sponsored",  EnumRel.Enum.SPONSORED_VALUE )
  );

  private static <T> ObjectOpenHashSet<T> makeSet( T... elements ) {
    return new ObjectOpenHashSet<>( elements );
  }

  private static <K> Map.Entry<K,Integer> E( final K key, final int value ) {
    return new java.util.AbstractMap.SimpleImmutableEntry<>( key, value );
  }

  private static <K> Object2IntOpenHashMap<K> makeMap( Map.Entry<K,Integer>... entries ) {
    final Object2IntOpenHashMap<K> map = new Object2IntOpenHashMap<>();
    for ( final Map.Entry<K,Integer> e : entries )
      map.put( e.getKey(), (int)e.getValue() );
    return map;
  }

  private static final class HttpLinksHeaderParser
  {
    private static final Logger LOGGER = LoggerFactory.getLogger( HttpLinksHeaderParser.class );
    private static final Pattern PATTERN_ELT = Pattern.compile( "<([^>]+)>((?:\\s*;\\s*[^=\\s,;]+\\s*=\\s*(?:\"[^\"]+\"|[^\\s,;]+))*)" );
    private static final Pattern PATTERN_PARAM = Pattern.compile( "([^=\\s,;]+)\\s*=\\s*(?:\"([^\"]+)\"|([^\\s,;=<>]+))" );

    static Iterable<HTMLLink> tryParse( final String elements ) {
      final var links = new ArrayList<HTMLLink>(2);
      final var matchElt = PATTERN_ELT.matcher( elements );
      while ( matchElt.find() ) {
        final var href = matchElt.group(1);
        final var link = tryGetLink( href, matchElt.group(2) );
        links.add( link );
      }
      return links;
    }

    private static HTMLLink tryGetLink( final String href, final String params ) {
      final var map = new HashMap<String, String>();
      if ( params != null ) {
        final var matchParam = PATTERN_PARAM.matcher( params );
        while ( matchParam.find() ) {
          final var key = matchParam.group(1);
          final var value = or( matchParam.group(2), matchParam.group(3) );
          if ( key != null && value != null )
            map.putIfAbsent( key, value );
        }
      }
      return new HTMLLink( HTMLLink.Type.LINK, href, map.get("title"), null, map.get("rel") );
    }
  }

  private static void test( final String str ) {
    final var matchElt = HttpLinksHeaderParser.PATTERN_ELT.matcher( str );
    while ( matchElt.find() ) {
      final var uri = matchElt.group(1);
      LOGGER.info( "uri: '{}'", uri );
      final var matchParam = HttpLinksHeaderParser.PATTERN_PARAM.matcher( matchElt.group(2) );
      while ( matchParam.find() ) {
        final var key = matchParam.group(1);
        final var value = or( matchParam.group(2), matchParam.group(3) );
        LOGGER.info( "  key: '{}', value: '{}'", key, value );
      }
    }
  }

  private static String or( final String left, final String right ) {
    return left != null ? left : right;
  }

  public static void main( final String[] args ) {
    final var tests = List.of(
      "</node/3>; rel=\"shortlink\",</page/cgu>; rel=\"canonical\"",
      "<https://festival.lemonde.fr/wp-json/>; rel=\"https://api.w.org/\", <https://festival.lemonde.fr/>; rel=shortlink",
      "<https://www.mangopay.com/fr/?p=807>; rel=shortlink",
      "<toto>",
      "<>; rel=pouet"
    );

    for ( final var test : tests ) {
      LOGGER.info( "test: '{}'", test );
      final var links = fromHttpHeader( test );
      for ( final var link : links ) {
        LOGGER.error( "'{}' [rel:{}]", link.uri, link.rel );
      }
    }

    //for ( var test : tests ) {
    //  LOGGER.info( "test: '{}'", test );
    //  test( test );
    //}
  }
}
