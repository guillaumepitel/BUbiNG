package it.unimi.di.law.bubing.util.detection;

public class ETagInfo
{
  public String httpHeaderETag = null;
  public String htmlMetaETag = null;


  @Override
  public String toString() {
    return httpHeaderETag + "," + htmlMetaETag;
  }
}
