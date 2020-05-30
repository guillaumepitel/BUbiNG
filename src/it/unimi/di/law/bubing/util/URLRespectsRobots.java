package it.unimi.di.law.bubing.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/*
 * Copyright (C) 2012-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

import it.unimi.di.law.warc.filters.URIResponse;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

//RELEASE-STATUS: DIST

/** A class providing static methods to parse <code>robots.txt</code> into arrays of char arrays and
 * handle robot filtering. */

public class URLRespectsRobots {
	private final static Logger LOGGER = LoggerFactory.getLogger(URLRespectsRobots.class);

	/** The maximum number of robots entries returned by {@link #toString()}. */
	public static final int MAX_TO_STRING_ROBOTS = 30;

	private URLRespectsRobots() {}

	public static class RobotsRules {
		public char[][] allow;
		public char[][] disallow;

		public RobotsRules() {
			allow = new char[0][];
			disallow = new char[0][];
		}
		public RobotsRules(char[][] allow, char[][] disallow) {
			this.allow = allow;
			this.disallow = disallow;
		}
	}

	/** A singleton empty robots filter. */
	public final static RobotsRules EMPTY_ROBOTS_FILTER = new RobotsRules();


	public static char[][] toSortedPrefixFreeCharArrays(final Set<String> set) {
		final int size = set.size();
		final String[] s = set.toArray(new String[size]);
		Arrays.sort(s);
		int j = 0;
		if (size != 0) {
			for(int i = 1; i < size; i++) if (! s[i].startsWith(s[j]))  s[++j] = s[i];
			++j;
		}
		final char[][] result = new char[j][];
		for(int i = 0; i < j; i++) result[i] = s[i].toCharArray();
		return result;
	}

	/** Parses the argument as if it were the content of a <code>robots.txt</code> file,
	 * and returns a sorted array of prefixes of URLs that the agent should not follow.
	 *
	 * @param content the content of the  <code>robots.txt</code> file.
	 * @param userAgent the string representing the user agent of interest.
	 * @return an array of character arrays, which are prefixes of the URLs not to follow, in sorted order.
	 */
	public static RobotsRules parseRobotsReader(final Reader content, final String userAgent, final MutableInt crawlDelay) throws IOException {
		/* The set of disallowed paths specifically aimed at userAgent. */
		Set<String> setDisallow = new ObjectOpenHashSet<>();
		/* The set of disallowed paths specifically aimed at *. */
		Set<String> setDisallowStar = new ObjectOpenHashSet<>();
		/* The set of disallowed paths specifically aimed at userAgent. */
		Set<String> setAllow = new ObjectOpenHashSet<>();
		/* The set of disallowed paths specifically aimed at *. */
		Set<String> setAllowStar = new ObjectOpenHashSet<>();
		/* True if the currently examined record is targetted to us. */
		boolean doesMatter = false;
		/* True if we have seen a section targetted to our agent. */
		boolean specific = false;
		/* True if we have seen a section targetted to *. */
		boolean generic = false;
		/* True if we are in a star section. */
		boolean starSection = false;

		StreamTokenizer st = new StreamTokenizer(new FastBufferedReader(content));
		int token;

		st.resetSyntax();
		st.eolIsSignificant(true); // We need EOLs to separate records
		st.wordChars(33, 255); // All characters may appear
		st.whitespaceChars(0, 32);
		st.ordinaryChar('#'); // We must manually simulate comments 8^(
		st.ordinaryChar(':');
		st.lowerCaseMode(false);
		int crawlDelayStar = 0;
		int ourCrawlDelay = 0;

		while (true) {
			int lineFirstToken = st.nextToken();
			if (lineFirstToken == StreamTokenizer.TT_EOF) break;

 			switch (lineFirstToken) {
 				// Blank line: a new block is starting
				case StreamTokenizer.TT_EOL:
					doesMatter = false;
					break;

				// Comment or number: ignore until the end of line
				case StreamTokenizer.TT_NUMBER:
				case '#':
					do {
						token = st.nextToken();
					} while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF);
					break;
				// A string
				case StreamTokenizer.TT_WORD:
					if (st.sval.equalsIgnoreCase("crawl-delay")) {
						token = st.nextToken(); // should be :
						if (token == ':') {
							token = st.nextToken();
							if (token == StreamTokenizer.TT_WORD) {

								try {
									int cd = Integer.valueOf(st.sval);
									if (doesMatter && specific)
										ourCrawlDelay = cd;
									if (starSection)
										crawlDelayStar = cd;
								} catch (NumberFormatException nfe) {
								}
							}
						}
						// Ignore the rest of the line
						while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF)
							token = st.nextToken();
					} else if (st.sval.equalsIgnoreCase("user-agent")) {
						token = st.nextToken();
						if (token == ':') {
							token = st.nextToken();
							if (token == StreamTokenizer.TT_WORD)
								if (StringUtils.startsWithIgnoreCase(userAgent, st.sval)) {
									doesMatter = true;
									specific = true;
									starSection = false;
								} else if (st.sval.equals("*")) {
									starSection = true;
									generic = true;
								} else starSection = false;
						}
						// Ignore the rest of the line
						while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF)
							token = st.nextToken();
					} else if (st.sval.equalsIgnoreCase("disallow")) {
						token = st.nextToken();
						if (token == ':') {
							token = st.nextToken();
							//System.out.println(st.sval + " " + starSection + " " + set + " " + setStar);
							if (token == StreamTokenizer.TT_EOL) {
								if (doesMatter) setDisallow.clear();
								else if (starSection) setDisallowStar.clear();
							} else if (token == StreamTokenizer.TT_WORD) {
								String disallowed = st.sval;
								if (disallowed.endsWith("*"))
									disallowed = disallowed.substring(0, disallowed.length() - 1); // Someone (erroneously) uses * to denote any suffix
								if (doesMatter) setDisallow.add(disallowed);
								else if (starSection) setDisallowStar.add(disallowed);
							}
						}
						// Ignore the rest of the line
						while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF)
							token = st.nextToken();
					} else if (st.sval.equalsIgnoreCase("allow")) {
						token = st.nextToken();
						if (token == ':') {
							token = st.nextToken();
							//System.out.println(st.sval + " " + starSection + " " + set + " " + setStar);
							if (token == StreamTokenizer.TT_EOL) {
								if (doesMatter) setAllow.clear();
								else if (starSection) setAllowStar.clear();
							} else if (token == StreamTokenizer.TT_WORD) {
								String allowed = st.sval;
								if (allowed.endsWith("*"))
									allowed = allowed.substring(0, allowed.length() - 1); // Someone (erroneously) uses * to denote any suffix
								if (doesMatter) setAllow.add(allowed);
								else if (starSection) setAllowStar.add(allowed);
							}
						}
						// Ignore the rest of the line
						while (token != StreamTokenizer.TT_EOL && token != StreamTokenizer.TT_EOF)
							token = st.nextToken();
					} else if (LOGGER.isTraceEnabled()) LOGGER.trace("Line first token {} ununderstandable in robots.txt", st.sval);
					break;

				// Something else: a syntax error
				default:
					if (LOGGER.isTraceEnabled()) LOGGER.trace("Found unknown token type {} in robots.txt", Integer.valueOf(lineFirstToken));
			}
		}
		if (specific)
			crawlDelay.setValue(ourCrawlDelay > 0 ? ourCrawlDelay:crawlDelayStar);
		else
			crawlDelay.setValue(crawlDelayStar);

		if (specific)
			return new RobotsRules(toSortedPrefixFreeCharArrays(setAllow),toSortedPrefixFreeCharArrays(setDisallow)); // Some instructions specific to us
		if (! specific && generic)
			return new RobotsRules(toSortedPrefixFreeCharArrays(setAllowStar),toSortedPrefixFreeCharArrays(setDisallowStar)); // No specific instruction, but some generic ones
		return new RobotsRules(toSortedPrefixFreeCharArrays(setAllow),toSortedPrefixFreeCharArrays(setDisallow));
	}

	/** Parses a <code>robots.txt</code> file contained in a {@link FetchData} and
	 * returns the corresponding filter as an array of sorted prefixes. HTTP statuses
	 * different from 2xx are {@linkplain Logger#warn(String) logged}. HTTP statuses of class 4xx
	 * generate an empty filter. HTTP statuses 2xx/3xx cause the tentative parsing of the
	 * request content. In the remaining cases we return {@code null}.
	 *
	 * @param robotsResponse the response containing <code>robots.txt</code>.
	 * @param userAgent the string representing the user agent of interest.
	 * @return an array of character arrays, which are prefixes of the URLs not to follow, in sorted order,
	 * or {@code null}
	 */
	public static RobotsRules parseRobotsResponse(final URIResponse robotsResponse, final String userAgent, final MutableInt crawlDelay) throws IOException {
		final int status = robotsResponse.response().getStatusLine().getStatusCode();
		if (status / 100 != 2) LOGGER.info("Got status " + status + " while fetching robots: URL was " + robotsResponse.uri());
		if (status / 100 == 4 || status / 100 == 5) return EMPTY_ROBOTS_FILTER; // For status 4xx and 5xx, we consider everything allowed.
		if (status / 100 != 2 && status / 100 != 3) return null; // For status 2xx and 3xx we parse the content. For the rest, we consider everything forbidden.
		// See if BOM is present and compute its length
		BOMInputStream bomInputStream = new BOMInputStream(robotsResponse.response().getEntity().getContent(), true);
		int bomLength = bomInputStream.hasBOM()? bomInputStream.getBOM().length() : 0;
		// Skip BOM, if necessary
		bomInputStream.skip(bomLength);
		// Parse robots (BOM is ignored, robots are UTF-8, as suggested by https://developers.google.com/search/reference/robots_txt
		RobotsRules result = parseRobotsReader(new InputStreamReader(bomInputStream, Charsets.UTF_8), userAgent, crawlDelay);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Robots for {} successfully got with status {}: Allow {} / Disallow {}", robotsResponse.uri(), Integer.valueOf(status),
			toString(result.allow), toString(result.disallow));
		return result;
	}

	private static int compare(char[] left, String right) {
		int l = Math.min(left.length, right.length());
		for (int i = 0; i < l; i++) {
			final int result = left[i] - right.charAt(i);
			if (result != 0) return result;
		}
		return left.length - right.length();
	}

	private static boolean doesNotStartsWith(final String s, final char[] prefix) {
		if (prefix.length > s.length()) return true;
		for (int i = prefix.length; i-- != 0;) if (s.charAt(i) != prefix[i]) return true;
		return false;
	}

	/** Checks whether a specified URL passes a specified robots filter.
	 *
	 * @param robotsFilter a robot filter.
	 * @param url a URL to check against {@code robotsFilter}.
	 * @return true if {@code url} passes {@code robotsFilter}.
	 */
	private static boolean apply(final char[][] robotsFilter, final URI url) {
		if (robotsFilter.length == 0) return true;
		final String pathQuery = BURL.pathAndQuery(url);
		int from = 0;
		int to = robotsFilter.length - 1;
		while (from <= to) {
			final int mid = (from + to) >>> 1;
			final int cmp = compare(robotsFilter[mid], pathQuery);
			if (cmp < 0) from = mid + 1;
			else if (cmp > 0) to = mid - 1;
			else return false; // key found (unlikely, but possible)
		}
		return from == 0 || doesNotStartsWith(pathQuery, robotsFilter[from-1]);
	}


	/** Checks whether a specified URL passes a specified robots filter.
	 *
	 * @param robotsFilter a robot filter.
	 * @param url a URL to check against {@code robotsFilter}.
	 * @return true if {@code url} passes {@code robotsFilter}.
	 */
	public static boolean apply(final RobotsRules robotsFilter, final URI url) {
		// To pass, a url can either not match a disallow rule, or match a disallow rule AND match and allow rule.
		return apply(robotsFilter.disallow, url) || !apply(robotsFilter.allow, url);
	}

	/** Prints gracefully a robot filter using at most {@value #MAX_TO_STRING_ROBOTS} prefixes.
	 *
	 * @param robotsFilter a robots filter.
	 * @return a string describing the filter.
	 */
	public static String toString(final char[][] robotsFilter) {
		if (robotsFilter == null) return "[]";
		final StringBuilder stringBuilder = new StringBuilder().append('[');
		final int n = Math.min(robotsFilter.length, MAX_TO_STRING_ROBOTS);
		for(int i = 0; i < n; i++){
			if (i != 0) stringBuilder.append(",");
			stringBuilder.append('"').append(robotsFilter[i]).append('"');
		}
		if (n != robotsFilter.length) stringBuilder.append(",...");
		return stringBuilder.append(']').toString();
	}

	public static void main(String arg[]) throws IOException {
		MutableInt crawlDelay = new MutableInt(0);
		RobotsRules robotsResult = URLRespectsRobots.parseRobotsReader(new FileReader(arg[0]), arg[1], crawlDelay);
		for(char[] a: robotsResult.allow) System.err.println(new String(a));
		for(char[] a: robotsResult.disallow) System.err.println(new String(a));

		final FastBufferedReader in = new FastBufferedReader(new InputStreamReader(System.in, Charsets.US_ASCII));
		final MutableString s = new MutableString();
		System.out.println("Crawl-Delay : " + crawlDelay.intValue());
		System.out.println("Please enter urls to test:");
		while(in.readLine(s) != null) {
			final URI uri = BURL.parse(s);
			if (apply(robotsResult,uri))
				System.out.println("URL " + uri + " allowed");
			else
				System.out.println("URL " + uri + " not allowed");
		}
		System.out.println("Crawl-Delay : " + crawlDelay.intValue());
		in.close();

	}
}
