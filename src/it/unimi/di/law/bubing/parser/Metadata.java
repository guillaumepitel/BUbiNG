package it.unimi.di.law.bubing.parser;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public final class Metadata
{
  private final Object2ObjectOpenHashMap<String, List<String>> map;

  public Metadata() {
    this.map = new Object2ObjectOpenHashMap<>();
  }

  private Metadata( final Object2ObjectOpenHashMap<String, List<String>> map ) {
    this.map = map;
  }

  public void set( final String key, final String value ) {
    if ( value == null )
      map.remove( key );
    else
      put( key, value );
  }

  public void add( final String key, final String value ) {
    if ( value != null ) {
      final List<String> existing = map.get( key );
      if ( existing == null )
        put( key, value );
      else
        existing.add( value );
    }
  }

  public String get( final String key ) {
    final List<String> values = map.get( key );
    return values == null ? null : values.get(0);
  }

  public List<String> getValues( final String key ) {
    return map.get( key );
  }

  public void clear() {
    map.clear();
  }

  public Iterable<String> keys() {
    return map.keySet();
  }

  public Iterable<List<String>> values() {
    return map.values();
  }

  public Iterable<? extends Map.Entry<String,List<String>>> entries() {
    return new Iterable<Object2ObjectOpenHashMap.Entry<String,List<String>>>() {
      @NotNull
      @Override
      public Iterator<Object2ObjectOpenHashMap.Entry<String,List<String>>> iterator() {
        return map.object2ObjectEntrySet().fastIterator();
      }
    };
  }

  private void put( final String key, final String value ) {
    final List<String> values = new ArrayList<>(2);
    values.add( value );
    map.put( key, values );
  }
}
