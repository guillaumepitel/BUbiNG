package it.unimi.di.law.bubing.frontier.comm;

import com.exensa.wdl.common.PartitionScheme;
import com.exensa.wdl.protobuf.url.MsgURL;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.frontier.Frontier;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

  public Consumer<byte[]> getCrawlRequestConsumer() {
    return crawlRequestConsumerRepository.get();
  }

  public void createFetchInfoProducers() {
    fetchInfoProducerRepository.requestProducers( client );
  }

  public void createCrawlRequestConsumers( final Frontier frontier ) {
    crawlRequestConsumerRepository.requestConsumer(client, frontier, rc.priorityCrawl);
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
      return get( partitionScheme.getHostPartition(urlKey) );
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
        .blockIfQueueFull( true )
        .maxPendingMessages(2048)
        .maxPendingMessagesAcrossPartitions(32768)
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
    private CompletableFuture<Consumer<byte[]>> future;
    private Consumer<byte[]> consumer;

    private CrawlRequestConsumerRepository() {
      this.future = null;
      this.consumer = null;
    }

    public Consumer<byte[]> get() {
      if (consumer != null)
        return consumer;
      return getSlowPath();
    }

    public void close() throws InterruptedException {
      closeConsumer();
      closeFuture();
    }

    private void closeConsumer() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of(consumer)
            .filter( java.util.Objects::nonNull)
            .map(Consumer::closeAsync)
            .toArray(CompletableFuture[]::new)
        ).get(5, TimeUnit.SECONDS);
      }
      catch (TimeoutException e) {
        LOGGER.error("Timeout while waiting for CrawlRequest consumers to close", e);
      }
      catch (ExecutionException e) {
        LOGGER.error("Failed to close CrawlRequest consumers", e);
      }
    }

    private void closeFuture() throws InterruptedException {
      try {
        CompletableFuture.allOf(
          java.util.stream.Stream.of(future)
            .filter(java.util.Objects::nonNull)
            .filter((f) -> !f.cancel(true))
            .map((f) -> f.getNow(null))
            .filter(java.util.Objects::nonNull)
            .map(Consumer::closeAsync)
            .toArray(CompletableFuture[]::new)
        ).get(5, TimeUnit.SECONDS);
      }
      catch (TimeoutException e) {
        LOGGER.error("Timeout while waiting for CrawlRequest consumers to close", e);
      }
      catch (ExecutionException e) {
        LOGGER.error("Failed to close CrawlRequest consumers", e);
      }
    }

    private Consumer<byte[]> getSlowPath() {
      consumer = getSlowPathImpl();
      future = null;
      return consumer;
    }

    private Consumer<byte[]> getSlowPathImpl() {
      int retry = 0;
      while (true) {
        try {
          return future.get(1, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
          LOGGER.warn(String.format("Timeout while creating CrawlRequest consumer %s", retry == 0 ? "" : String.format(" (%d)", retry)));
          retry += 1;
        }
        catch ( InterruptedException|ExecutionException e ) {
          LOGGER.error("While creating CrawlRequest consumer", e);
          return null;
        }
      }
    }

    private void requestConsumer(final PulsarClient client, final Frontier frontier, final boolean priorityCrawl) {
      String topicName;
      if (priorityCrawl)
        topicName = String.format("%s", rc.pulsarFrontierToPromptlyCrawlURLsTopic);
      else
        topicName = String.format("%s", rc.pulsarFrontierToCrawlURLsTopic);

      future = client.newConsumer()
        .subscriptionType(SubscriptionType.Key_Shared)
        //.receiverQueueSize(512)
        //.maxTotalReceiverQueueSizeAcrossPartitions(4096)
        //.acknowledgmentGroupTime(500, TimeUnit.MILLISECONDS)
        .messageListener(new CrawlRequestsReceiver(frontier))
        .subscriptionName("toCrawlSubscription")
        .consumerName(rc.name)
        .topic(topicName)
        .subscribeAsync();
      LOGGER.warn("Requested creation of CrawlRequest consumer for topic {}", topicName);
    }
  }
}
