package it.unimi.di.law.bubing.parser;

import com.exensa.wdl.protobuf.crawler.MsgCrawler;
import it.unimi.di.law.warc.filters.Filter;
import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.lang.FlyweightPrototype;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.net.URI;

/*
 * Copyright (C) 2004-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

// RELEASE-STATUS: DIST

/** A generic parser for {@link HttpResponse responses}. Every parser provides the following functionalities:
 *  <ul>
 *  	<li>it acts as a {@link Filter} that is able to decide whether it can parse a certain {@link URIResponse} or not (e.g.,
 *  	based on the declared <code>content-type</code> header etc.);
 *  	<li>while {@linkplain #parse(URI, HttpResponse, MsgCrawler.FetchInfo.Builder) parsing},
 *  	it will send the links found in the document to the
 *  	specified {@link MsgCrawler.FetchInfo.Builder)}, that will typically accumulate them or send them to the appropriate class for processing;
 *  	<li>the {@link #parse(URI, HttpResponse, MsgCrawler.FetchInfo.Builder)) parsing} method will return a digest computed on a
 *  	(possibly) suitably modified version of the document (the way in which the document it is actually modified and
 *		the way in which the hash is computed is implementation-dependent and should be commented by the implementing classes);
 *  	<li>after parsing, a {@linkplain #guessedCharset() guess of the charset used for the document} will be made available.
 *  </ul>
 */
public interface Parser<T> extends Filter<URIResponse> {


  /**
	 * A class that can receive piece of text discovered during parsing.
	 */
	public static interface TextProcessor<T> extends Appendable, FlyweightPrototype<TextProcessor<T>> {
		/**
		 * Initializes this processor for a new page.
		 *
		 * @param responseUrl the URL of the page to be parsed.
		 */
		public void init(URI responseUrl);

		/**
		 * Returns the result of the processing.
		 * @return the result of the processing.
		 */
		public T result();
	}


	/**
	 * Parses a response.
	 *
	 * @param response a response to parse.
	 * @return a digest of the page content, or {@code null} if no digest has been
	 * computed.
	 */
	public ParseData parse(final URI uri, final HttpResponse response) throws IOException;

	/**
	 * Returns a guessed charset for the document, or {@code null} if the charset could not be
	 * guessed.
	 *
	 * @return a charset or {@code null}.
	 */
	//public Charset guessedCharset();
  //
	//public Locale guessedLanguage();
  //
	//public CharsetDetectionInfo getCharsetDetectionInfo();
  //
	//public LanguageDetectionInfo getLanguageDetectionInfo();
  //
	//public URI getBase();
  //
	//public List<HTMLLink> getLinks();
  //
	///**
	// * Return raw content (without scripts etc. & html entities) in byte form in order to detect encoding when none is provided
	// *
	// * @return the cleaned page content or {@code null}.
	// */
	//public StringBuilder getRewrittenContent();
  //
	///**
	// * Return text content (without html tags & html entities)
	// *
	// * @return the cleaned page content or {@code null}.
	// */
	//public StringBuilder getTextContent();
  //
	//public String getTitle();
  //
  //
	///**
	// *
	// *
	// * @return a boolean which indicate if the html contains a rendering mode for narrow screen
	// */
	//public Boolean responsiveDesign();
  //
	///**
	// *
	// *
	// * @return a boolean which indicates if the html version is at least 5. Return false if the page can't be parse.
	// */
	//public Boolean html5();


	/**
	 * Returns the result of the processing.
	 *
	 * <p>Note that this method must be idempotent.
	 *
	 * @return the result of the processing.
	 */
	public T result();

	/** This method strengthens the return type of the method inherited from {@link Filter}.
	 *
	 * @return  a copy of this object, sharing state with this object as much as possible.
	 */
	@Override
	public Parser<T> copy();
}
