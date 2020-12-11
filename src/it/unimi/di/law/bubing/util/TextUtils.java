package it.unimi.di.law.bubing.util;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.regex.Pattern;


public class TextUtils
{
  private static final Pattern PATTERN_MARK = Pattern.compile( "\\p{M}", Pattern.UNICODE_CHARACTER_CLASS );
  private static final Pattern PATTERN_ANY_NOT_ALNUM = Pattern.compile( "[^\\p{Alnum}]+", Pattern.UNICODE_CHARACTER_CLASS );
  private static final Pattern PATTERN_ANY_WSPACE = Pattern.compile( "\\s+", Pattern.UNICODE_CHARACTER_CLASS );

  public static String[] splitLine( final String line, final boolean lowerCase ) {
    String cleaned = clean( line, lowerCase );
    if (cleaned.equals(""))
      return new String[0];
    return cleaned.split( " " );
  }

  public static String clean( final String input, final boolean lowerCase ) {
    return removeExtraSpaces( unponctuate(
      undiacritics( lowerCase
        ? input.toLowerCase(Locale.ROOT)
        : input )
    ) );
  }

  public static String undiacritics( final String input ) {
    final String normalized = Normalizer.normalize( input, Normalizer.Form.NFD );
    return PATTERN_MARK.matcher( normalized ).replaceAll( "" );
  }

  public static String unponctuate( final String input ) {
    return PATTERN_ANY_NOT_ALNUM.matcher( input ).replaceAll( " " );
  }

  public static String removeExtraSpaces( final String input ) {
    return PATTERN_ANY_WSPACE.matcher( input ).replaceAll( " " ).trim();
  }

  public static double computeTextQuality( final String text ) {
    return computeTextQuality(
      splitLine( text, true )
    );
  }

  public static double computeTextQuality( final String[] content ) {
    if ( content.length == 0 )
      return 0.0;
    final HashSet<String> vocabulary = new HashSet<>( Arrays.asList(content) );
    return Math.pow(vocabulary.size(),0.5) / Math.pow(content.length,0.4);
  }
}
