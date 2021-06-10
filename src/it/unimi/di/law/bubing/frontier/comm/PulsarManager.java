package it.unimi.di.law.bubing.frontier.comm;

import com.exensa.wdl.common.PartitionScheme;
import com.exensa.wdl.protobuf.url.MsgURL;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.Frontier;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;

public final class PulsarManager implements AutoCloseable
{
  private static final Logger LOGGER = LoggerFactory.getLogger( PulsarManager.class );

  private final RuntimeConfiguration rc;
  private PulsarClient client;
  private final FetchInfoProducerRepository fetchInfoProducerRepository;
  private final CrawlRequestConsumerRepository crawlRequestConsumerRepository;
  private final PartitionScheme partitionScheme;

  public PulsarManager( final RuntimeConfiguration rc ) throws PulsarClientException {
    this.rc = rc;
    this.client = createClient( rc );
    this.fetchInfoProducerRepository = new FetchInfoProducerRepository();
    this.crawlRequestConsumerRepository = new CrawlRequestConsumerRepository();
    this.partitionScheme = new PartitionScheme(rc.pulsarFrontierTopicNumber);
  }

  public Producer<byte[]> getFetchInfoProducer( final MsgURL.Key urlKey ) {
    return fetchInfoProducerRepository.get( urlKey );
  }

  public Consumer<byte[]> getCrawlRequestConsumer( final int topic ) {
    return crawlRequestConsumerRepository.get( topic );
  }

  public void createFetchInfoProducers() {
    fetchInfoProducerRepository.requestProducers( client );
  }

  public void createCrawlRequestConsumers( final Frontier frontier ) {
    crawlRequestConsumerRepository.requestConsumers(client, frontier, rc.priorityCrawl);
  }

  public void close() throws InterruptedException {
    closeCrawlRequestConsumers();
    closeFetchInfoProducers();
    closePulsarClient();
  }

  public void closeFetchInfoProducers() throws InterruptedException {
    fetchInfoProducerRepository.close();
  }

  public void closeCrawlRequestConsumers() throws InterruptedException {
    crawlRequestConsumerRepository.close();
  }

  public void closePulsarClient() throws InterruptedException {
    try {
      client.closeAsync().get();
    }
    catch ( ExecutionException e ) {
      LOGGER.error( "While closing pulsar client", e );
    }
  }

  private static PulsarClient createClient( final RuntimeConfiguration rc ) throws PulsarClientException {
    return PulsarClient.builder()
      .ioThreads( 16 )
      .listenerThreads( 16 )
      .connectionsPerBroker( 4 )
      .enableTls( false )
      .enableTlsHostnameVerification( false )
      .statsInterval( 10, TimeUnit.MINUTES )
      .serviceUrl( rc.pulsarClientConnection )
      .build();
  }

  private final class FetchInfoProducerRepository
  {
    private final CompletableFuture<Producer<byte[]>>[] futures;
    private final Producer<byte[]>[] producers;

    private FetchInfoProducerRepository() {
      this.futures = new CompletableFuture[ rc.pulsarFrontierTopicNumber ];
      this.producers = new Producer[ rc.pulsarFrontierTopicNumber ];
    }

    public Producer<byte[]> get( final int topic ) {
      if ( producers[topic] != null )
        return producers[topic];
      return getSlowPath( topic );
    }

    public Producer<byte[]> get( final MsgURL.Key urlKey ) {
      return get( partitionScheme.getPartition(urlKey) );
    }

    public void close() throws InterruptedException {
      closeProducers();
      closeFutures();
    }

    private void closeProducers() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of( producers )
            .filter( java.util.Objects::nonNull )
            .map( Producer::closeAsync )
            .toArray( CompletableFuture[]::new )
        ).get( 5, TimeUnit.SECONDS );
      }
      catch ( TimeoutException e ) {
        LOGGER.error( "Timeout while waiting for FetchInfo producers to close", e );
      }
      catch ( ExecutionException e ) {
        LOGGER.error( "Failed to close FetchInfo producers", e );
      }
    }

    private void closeFutures() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of( futures )
            .filter( java.util.Objects::nonNull )
            .filter( (f) -> !f.cancel(true) )
            .map( (f) -> f.getNow(null) )
            .filter( java.util.Objects::nonNull )
            .map( Producer::closeAsync )
            .toArray( CompletableFuture[]::new )
        ).get( 5, TimeUnit.SECONDS );
      }
      catch ( TimeoutException e ) {
        LOGGER.error( "Timeout while waiting for FetchInfo producers to close", e );
      }
      catch ( ExecutionException e ) {
        LOGGER.error( "Failed to close FetchInfo producers", e );
      }
    }

    private Producer<byte[]> getSlowPath( final int topic ) {
      final Producer<byte[]> producer = getSlowPathImpl( topic );
      // if fail to get producer, expect NPE later
      producers[topic] = producer;
      futures[topic] = null;
      return producer;
    }

    private Producer<byte[]> getSlowPathImpl( final int topic ) {
      int retry = 0;
      while ( true ) {
        try {
          return futures[topic].get( 1, TimeUnit.SECONDS );
        }
        catch ( TimeoutException e ) {
          LOGGER.warn(String.format( "Timeout while creating FetchInfo producer [%d]%s", topic, retry == 0 ? "" : String.format(" (%d)",retry)  ));
          retry += 1;
        }
        catch ( InterruptedException|ExecutionException e ) {
          LOGGER.error(String.format( "While creating FetchInfo producer [%d]", topic), e);
          return null;
        }
      }
    }

    private void requestProducers( final PulsarClient client ) {
      final ProducerBuilder<byte[]> producerBuilder = client.newProducer()
        .enableBatching( true )
        .batchingMaxPublishDelay( 100, TimeUnit.MILLISECONDS )
        .batchingMaxMessages(16)
        .blockIfQueueFull( true )
        .maxPendingMessages(128)
        .maxPendingMessagesAcrossPartitions(1024)
        .sendTimeout( 30000, TimeUnit.MILLISECONDS )
        .compressionType( CompressionType.ZSTD )
        .producerName( rc.name );

      for ( int i=0; i<rc.pulsarFrontierTopicNumber; ++i )
        futures[i] = producerBuilder
          .topic(String.format( "%s-%d", rc.pulsarFrontierFetchTopic, i ))
          .createAsync();
      LOGGER.warn( "Requested creation of {} FetchInfo producers for topic {}", rc.pulsarFrontierTopicNumber, rc.pulsarFrontierFetchTopic);
    }
  }

  private final class CrawlRequestConsumerRepository
  {
    private final CompletableFuture<Consumer<byte[]>>[] futures;
    private final Consumer<byte[]>[] consumers;

    private CrawlRequestConsumerRepository() {
      this.futures = new CompletableFuture[ rc.pulsarFrontierTopicNumber ];
      this.consumers = new Consumer[ rc.pulsarFrontierTopicNumber ];
    }

    public Consumer<byte[]> get( final int topic ) {
      if ( consumers[topic] != null )
        return consumers[topic];
      return getSlowPath( topic );
    }

    public void close() throws InterruptedException {
      closeConsumers();
      closeFutures();
    }

    private void closeConsumers() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of( consumers )
            .filter( java.util.Objects::nonNull )
            .map( Consumer::closeAsync )
            .toArray( CompletableFuture[]::new )
        ).get( 5, TimeUnit.SECONDS );
      }
      catch ( TimeoutException e ) {
        LOGGER.error( "Timeout while waiting for CrawlRequest consumers to close", e );
      }
      catch ( ExecutionException e ) {
        LOGGER.error( "Failed to close CrawlRequest consumers", e );
      }
    }

    private void closeFutures() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of( futures )
            .filter( java.util.Objects::nonNull )
            .filter( (f) -> !f.cancel(true) )
            .map( (f) -> f.getNow(null) )
            .filter( java.util.Objects::nonNull )
            .map( Consumer::closeAsync )
            .toArray( CompletableFuture[]::new )
        ).get( 5, TimeUnit.SECONDS );
      }
      catch ( TimeoutException e ) {
        LOGGER.error( "Timeout while waiting for CrawlRequest consumers to close", e );
      }
      catch ( ExecutionException e ) {
        LOGGER.error( "Failed to close CrawlRequest consumers", e );
      }
    }

    private Consumer<byte[]> getSlowPath( final int topic ) {
      final Consumer<byte[]> consumer = getSlowPathImpl( topic );
      // if fail to get consumer, expect NPE later
      consumers[topic] = consumer;
      futures[topic] = null;
      return consumer;
    }

    private Consumer<byte[]> getSlowPathImpl( final int topic ) {
      int retry = 0;
      while ( true ) {
        try {
          return futures[topic].get( 1, TimeUnit.SECONDS );
        }
        catch ( TimeoutException e ) {
          LOGGER.warn(String.format( "Timeout while creating CrawlRequest consumer [%d]%s", topic, retry == 0 ? "" : String.format(" (%d)",retry)  ));
          retry += 1;
        }
        catch ( InterruptedException|ExecutionException e ) {
          LOGGER.error(String.format( "While creating CrawlRequest consumer [%d]", topic), e);
          return null;
        }
      }
    }

    private void requestConsumers(final PulsarClient client, final Frontier frontier, final boolean priorityCrawl) {
      for (int topic = 0; topic < rc.pulsarFrontierTopicNumber; ++topic)
        futures[topic] = requestConsumer(client, frontier, topic, priorityCrawl);
      String topicName;
      if (priorityCrawl)
        topicName = rc.pulsarFrontierToPromptlyCrawlURLsTopic;
      else
        topicName = rc.pulsarFrontierToCrawlURLsTopic;
      LOGGER.warn("Requested creation of {} CrawlRequest consumers for topic {}", rc.pulsarFrontierTopicNumber, topicName);
    }

    private String getConsumerName(final int topic) {
      // node X has consumerId ZERO for each topic such that
      // topic % nodeNunber == X
      // otherwise, the consumerId is 1 + random
      boolean isMyDefaultTopic =  ( topic % rc.pulsarFrontierNodeNumber ) == rc.pulsarFrontierNodeId;
      int consumerID = isMyDefaultTopic ? 0 :
        ThreadLocalRandom.current().nextInt(1, rc.pulsarFrontierNodeNumber*rc.pulsarFrontierTopicNumber);

      return String.format("%06d-%s", consumerID, rc.name);
    }

    private CompletableFuture<Consumer<byte[]>> requestConsumer(final PulsarClient client, final Frontier frontier,
                                                                final int topic, final boolean priorityCrawl) {
      String topicName;
      if (priorityCrawl)
        topicName = String.format("%s-%d", rc.pulsarFrontierToPromptlyCrawlURLsTopic, topic);
      else
        topicName = String.format("%s-%d", rc.pulsarFrontierToCrawlURLsTopic, topic);

      return client.newConsumer()
        .subscriptionType( SubscriptionType.Failover )
        .receiverQueueSize(64)
        .maxTotalReceiverQueueSizeAcrossPartitions(4096)
        .acknowledgmentGroupTime( 500, TimeUnit.MILLISECONDS )
        .messageListener( new CrawlRequestsReceiver(frontier,topic) )
        .subscriptionInitialPosition( SubscriptionInitialPosition.Latest )
        .subscriptionName( "toCrawlSubscription" )
        .consumerName(getConsumerName(topic))
        .topic(topicName)
        .subscribeAsync();
    }
  }
}
