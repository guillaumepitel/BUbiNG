package it.unimi.di.law.bubing.frontier.comm;

/*
 * Copyright (C) 2013-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//RELEASE-STATUS: DIST

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import it.unimi.di.law.bubing.frontier.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * A thread that takes care of sending the content of {@link Frontier#fetchInfoSendQueue} with submit().
 */

public final class FetchInfoSendThread extends Thread
{
  private static final Logger LOGGER = LoggerFactory.getLogger( FetchInfoSendThread.class );
  private static final int BULK_SIZE = 512;
  private static final long SLEEP_DURATION = 50;

  private final PulsarManager pulsarManager;
  private final ArrayBlockingQueue<MsgCrawler.FetchInfo> queue;

  public FetchInfoSendThread( final PulsarManager pulsarManager, final ArrayBlockingQueue<MsgCrawler.FetchInfo> queue ) {
    this.pulsarManager = pulsarManager;
    this.queue = queue;

    setName( this.getClass().getSimpleName() );
    setPriority( Thread.MAX_PRIORITY ); // This must be done quickly
  }

  /**
   * When set to true, this thread will complete its execution.
   */
  public volatile boolean stop = false;

  @Override
  public void run() {
    try {
      LOGGER.warn( "thread [started]" );
      final ArrayList<MsgCrawler.FetchInfo> bulk = new ArrayList<>( BULK_SIZE );

      boolean stopping = false;
      while ( true ) {
        if ( stop && !stopping ) {
          stopping = true;
          LOGGER.warn( "thread [stopping]" );
        }

        if ( queue.drainTo(bulk,BULK_SIZE) == 0 ) {
          if ( stopping ) break;
          Thread.sleep( SLEEP_DURATION );
          continue;
        }

        for ( final MsgCrawler.FetchInfo fetchInfo : bulk ) {
          pulsarManager
            .getFetchInfoProducer( fetchInfo.getUrlKey() )
            .sendAsync( fetchInfo.toByteArray() );
        }
        bulk.clear();
      }
    }
    catch ( InterruptedException e ) {
      LOGGER.error( "Interrupted", e );
    }
    catch ( Throwable t ) {
      LOGGER.error( "Unexpected error", t );
    }
    finally {
      LOGGER.warn( "thread [stopped]" );
    }
  }
}
