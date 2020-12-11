package it.unimi.di.law.bubing.util;

import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import it.unimi.di.law.warc.filters.URIResponse;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

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

//RELEASE-STATUS: DIST

/** A class providing static methods to parse <code>robots.txt</code> into arrays of char arrays and
 * handle robot filtering. */

public class URLRespectsRobots {
	private final static Logger LOGGER = LoggerFactory.getLogger(URLRespectsRobots.class);

	/** The maximum number of robots entries returned by {@link #toString()}. */

	private URLRespectsRobots() {}

	/** A singleton empty robots filter. */
	public final static SimpleRobotRules EMPTY_ROBOTS_FILTER = new SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL);

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
	private static SimpleRobotRulesParser robotsParser = new SimpleRobotRulesParser(100000,10);

	public static SimpleRobotRules parseRobotsResponse(final URIResponse robotsResponse, final String userAgent) throws IOException {
		final int status = robotsResponse.response().getStatusLine().getStatusCode();
		if (status / 100 != 2) LOGGER.info("Got status " + status + " while fetching robots: URL was " + robotsResponse.uri());
		if (status / 100 == 4 || status / 100 == 5) return EMPTY_ROBOTS_FILTER; // For status 4xx and 5xx, we consider everything allowed.
		if (status / 100 != 2 && status / 100 != 3) return null; // For status 2xx and 3xx we parse the content. For the rest, we consider everything forbidden.
		Header contentTypeHeader = robotsResponse.response().getFirstHeader(HttpHeaders.CONTENT_TYPE);
		String contentType = "";
		if (contentTypeHeader != null && contentTypeHeader.getValue() != null)
			contentType = contentTypeHeader.getValue();
		SimpleRobotRules result = robotsParser.parseContent(robotsResponse.uri().toString(),
			robotsResponse.response().getEntity().getContent().readAllBytes(),
		contentType, userAgent);
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Robots for {} successfully got with status {}: {}, CrawlDelay {}", robotsResponse.uri(), Integer.valueOf(status),
			result.toString(), result.getCrawlDelay());
		return result;
	}

	/** Checks whether a specified URL passes a specified robots filter.
	 *
	 * @param robotsFilter a robot filter.
	 * @param url a URL to check against {@code robotsFilter}.
	 * @return true if {@code url} passes {@code robotsFilter}.
	 */
	public static boolean apply(final SimpleRobotRules robotsFilter, final URI url) {
		return robotsFilter.isAllowed(url.toString());
	}
}
