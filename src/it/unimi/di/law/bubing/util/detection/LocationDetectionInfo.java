package it.unimi.di.law.bubing.util.detection;

import java.net.URI;


public class LocationDetectionInfo
{
  public URI httpHeaderLocation = null;
  public URI htmlRefreshLocation = null;
  public URI httpHeaderContentLocation = null;
  public URI httpHeaderRefreshLocation = null;

  @Override
  public String toString() {
    return toString( httpHeaderLocation ) +
      "," + toString(htmlRefreshLocation) +
      "," + toString(httpHeaderContentLocation) +
      "," + toString(httpHeaderRefreshLocation);
  }

  private static String toString( final URI uri ) {
    return uri == null ? "-" : uri.toString();
  }
}
