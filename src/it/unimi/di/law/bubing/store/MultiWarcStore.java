package it.unimi.di.law.bubing.store;

/*		 
 * Copyright (C) 2012-2013 Paolo Boldi, Massimo Santini, and Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import com.hadoop.compression.fourmc.ZstdCodec;
import it.unimi.di.law.bubing.RuntimeConfiguration;
import it.unimi.di.law.warc.io.UncompressedWarcWriter;
import it.unimi.di.law.warc.io.WarcWriter;
import it.unimi.di.law.warc.records.HttpResponseWarcRecord;
import it.unimi.di.law.warc.records.WarcHeader;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.http.HttpResponse;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;


//RELEASE-STATUS: DIST

/** A {@link Store} implementation using the {@link it.unimi.di.law.warc} package. */

public class MultiWarcStore implements Closeable, Store {
	private final static Logger LOGGER = LoggerFactory.getLogger( MultiWarcStore.class );

	private final int OUTPUT_STREAM_BUFFER_SIZE = 32*1024 * 1024;
	private final static String STORE_NAME_FORMAT = "store.warc.%s.%s.zstd_mc";
	private final static String URL_NAME_FORMAT = "urls.%s.%s.gz";
	public final static String DIGESTS_NAME = "digests.bloom";

	private int maxRecordsPerFile = 25600;
	private int maxSecondsBetweenDumps = 600;
	private int currentNumberOfRecordsInFile = 0;
	private long lastDumpTime = (new Date()).getTime()/1000;
	private Object counterLock = new Object();
	private FastBufferedOutputStream warcOutputStream;
	private WarcWriter warcWriter;
	private OutputStream urlOutputStream;
	private BufferedWriter urlWriter;
	private String currentStoreBaseName;
	private String currentUrlBaseName;
	private final File storeDir;
	private final CompressionCodec codec;

	public MultiWarcStore( final RuntimeConfiguration rc ) throws IOException {
		storeDir = rc.storeDir;
		maxRecordsPerFile = rc.maxRecordsPerFile;
		maxSecondsBetweenDumps = rc.maxSecondsBetweenDumps;
		LOGGER.debug("Max record per file = " + maxRecordsPerFile);
		LOGGER.debug("Max seconds between dumps = " + maxSecondsBetweenDumps);

		Configuration conf = new Configuration();
		conf.addResource(ClassLoader.getSystemResourceAsStream("core-site.xml"));
		CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
		codec = ccf.getCodecByClassName(ZstdCodec.class.getName());
		createNewWriter( );	
	}
	private String generateStoreName(Date d) {
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS");
        String datetime = ft.format(d);
		currentStoreBaseName = String.format(STORE_NAME_FORMAT, datetime, UUID.randomUUID());
		return currentStoreBaseName;
	}

	private String generateUrlName(Date d) {
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss.SSS");
		String datetime = ft.format(d);
		currentUrlBaseName = String.format(URL_NAME_FORMAT, datetime, UUID.randomUUID());
		return currentUrlBaseName;
	}

	private void createNewWriter() throws IOException {
		Date now = new Date();
		final File warcFile = new File( storeDir, "."+generateStoreName(now) );
		final File urlFile = new File( storeDir, "."+generateUrlName(now) );

		warcOutputStream = new FastBufferedOutputStream( codec.createOutputStream(new FileOutputStream( warcFile )), OUTPUT_STREAM_BUFFER_SIZE);
		warcWriter = new UncompressedWarcWriter(warcOutputStream);
		urlOutputStream = new FastBufferedOutputStream( new GZIPOutputStream(new FileOutputStream( urlFile )), OUTPUT_STREAM_BUFFER_SIZE );
		urlWriter = new BufferedWriter(new OutputStreamWriter(urlOutputStream, "UTF-8"));
	}
		
	@Override
	public void store(final URI uri, final HttpResponse response, final boolean isDuplicate, final byte[] contentDigest,
					  final String guessedCharset, final String guessedLanguage, final Map<String,String> extraHeaders,
					  final StringBuilder textContent) throws IOException, InterruptedException {
		if ( contentDigest == null ) throw new NullPointerException( "Content digest is null" );
		LOGGER.debug("MultiWarcStore:Store Uri = "+ uri.toString());
		final HttpResponseWarcRecord record = new HttpResponseWarcRecord( uri, response );
		HeaderGroup warcHeaders = record.getWarcHeaders();
		warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.WARC_PAYLOAD_DIGEST, "bubing:" + Hex.encodeHexString( contentDigest ) ) );
		if ( guessedCharset != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_CHARSET, guessedCharset ) );
		if ( guessedLanguage != null ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_GUESSED_LANGUAGE, guessedLanguage ) );

		for (String header: extraHeaders.keySet()) {
			warcHeaders.updateHeader(new BasicHeader(header, extraHeaders.get(header)));
		}
		if ( isDuplicate ) warcHeaders.updateHeader( new WarcHeader( WarcHeader.Name.BUBING_IS_DUPLICATE, "true" ) );
		synchronized(counterLock) {
			long currentTime = new Date().getTime()/1000;
			
			if (currentNumberOfRecordsInFile > 0 && ((currentNumberOfRecordsInFile > maxRecordsPerFile) || 
				(currentTime-lastDumpTime > maxSecondsBetweenDumps))) {
				LOGGER.debug("Current time = " + currentTime + ", lastDumpTime = " + lastDumpTime );
				currentNumberOfRecordsInFile = 0;
				lastDumpTime = currentTime;
				LOGGER.info( "Target number of records reached, creating new output file" );
				realClose();
				createNewWriter();
			}
			urlWriter.write(uri.toString() + '\n');
		}
		warcWriter.write( record );
		currentNumberOfRecordsInFile += 1;
	}

	private void realClose() throws IOException {
		try {
			warcWriter.close();
			urlWriter.close();
		}
		catch ( IOException shouldntHappen ) {
			LOGGER.error( "Interrupted while closing parallel output stream" );
		}
		warcOutputStream.close();
		urlOutputStream.close();
		Files.move(Paths.get(storeDir.getAbsolutePath(), "."+currentStoreBaseName),
				Paths.get(storeDir.getAbsolutePath(), currentStoreBaseName));
		Files.move(Paths.get(storeDir.getAbsolutePath(), "."+currentUrlBaseName),
				Paths.get(storeDir.getAbsolutePath(), currentUrlBaseName));
	}

	@Override
	public synchronized void close() throws IOException {
		realClose();
	}
}
