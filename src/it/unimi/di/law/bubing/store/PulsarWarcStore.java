package it.unimi.di.law.bubing.store;

import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.bubing.protobuf.WebPageTextContentProto;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

//RELEASE-STATUS: DIST

/** A {@link Store} implementation using Pulsar. */

public class PulsarWarcStore implements Closeable, Store {
	private final static Logger LOGGER = LoggerFactory.getLogger( PulsarWarcStore.class );

	private final PulsarClient pulsarClient;
	private final Producer pulsarWARCProducer;
	private final Producer pulsarPlainTextProducer;
	private final ByteArrayOutputStream outputStream;
	private final WarcWriter warcWriter;

	public PulsarWarcStore(final RuntimeConfiguration rc ) throws IOException {

		ClientBuilder clientBuilder = PulsarClient.builder()
      .ioThreads(8)
      .listenerThreads(8)
      .connectionsPerBroker(2)
      .enableTlsHostnameVerification(false)
      .serviceUrl(rc.pulsarClientConnection);
		pulsarClient = clientBuilder.build();

		ProducerBuilder producerBuilder = pulsarClient.newProducer()
			.enableBatching(true)
			.batchingMaxMessages(128)
			.batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
			.blockIfQueueFull(true)
			.sendTimeout(30000, TimeUnit.MILLISECONDS)
			.compressionType(CompressionType.LZ4)
			.producerName(rc.name + UUID.randomUUID().toString());

		pulsarWARCProducer = producerBuilder.topic(rc.pulsarWARCTopic).create();
		pulsarPlainTextProducer = producerBuilder.topic(rc.pulsarPlainTextTopic).create();

		outputStream = new ByteArrayOutputStream();
		warcWriter = new UncompressedWarcWriter(outputStream);
	}

	@Override
	public void store(final URI uri,
					  final HttpResponse response,
					  final boolean isDuplicate,
					  final byte[] contentDigest,
					  final String guessedCharset,
					  final String guessedLanguage,
					  final Map<String,String> extraHeaders,
					  final StringBuilder textContent) throws IOException, InterruptedException {
		if ( contentDigest == null ) throw new NullPointerException( "Content digest is null" );
		LOGGER.debug("PulsarWarc:Store Uri = "+ uri.toString());
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord( uri, response );
		HeaderGroup warcHeaders = record.getWarcHeaders();
		warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.WARC_PAYLOAD_DIGEST, "bubing:" + Hex.encodeHexString( contentDigest ) ) );
		if ( guessedCharset != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_CHARSET, guessedCharset ) );
		if ( guessedLanguage != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_LANGUAGE, guessedLanguage ) );

		for(String header: extraHeaders.keySet()) {
			warcHeaders.updateHeader(new BasicHeader(header, extraHeaders.get(header)));
		}
		if ( isDuplicate ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_IS_DUPLICATE, "true" ) );

		warcWriter.write( record );
		pulsarWARCProducer.sendAsync(outputStream.toByteArray());
		outputStream.reset();

		WebPageTextContentProto.WebPage.Builder webPageBuilder = WebPageTextContentProto.WebPage.newBuilder();
		if (textContent != null)
			webPageBuilder.setContent(textContent.toString());
		if (extraHeaders.get("BUbiNG-Guessed-Html5") != null)
			webPageBuilder.setHtml5(extraHeaders.get("BUbiNG-Guessed-Html5").equals("true"));
		if (extraHeaders.get("BUbiNG-Guessed-responsive") != null)
			webPageBuilder.setViewport(extraHeaders.get("BUbiNG-Guessed-responsive").equals("true"));

		pulsarPlainTextProducer.sendAsync(WebPageTextContentProto.WebPage.newBuilder()
				.setUrl(uri.toString())
				.setCharset(guessedCharset == null ? "" : guessedCharset)
				.setLang(guessedLanguage == null ? "" : guessedLanguage)
				.build()
				.toByteArray());
	}

	private void realClose() throws IOException {
		try {
			warcWriter.close();
		}
		catch ( IOException shouldntHappen ) {
			LOGGER.error( "Interrupted while closing parallel output stream" );
		}
		outputStream.close();
	}

	@Override
	public synchronized void close() throws IOException {
		realClose();
	}
}
