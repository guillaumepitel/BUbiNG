package it.unimi.di.law.bubing.util;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.client.RedirectException;
import org.junit.After;
import org.junit.Test;

public class URLRespectsRobotsTest {

	SimpleFixedHttpProxy proxy;


	@After
	public void tearDownProxy() throws InterruptedException, IOException {
		if (proxy != null) proxy.stopService();
	}

	@Test
	public void testDisallowEverytingSync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bar/robots.txt");
		proxy.add200(robotsURL, "",
				"# go away\n" +
						"User-agent: *\n" +
						"Disallow: /\n"
				);
		final URI disallowedUri1 = URI.create("http://foo.bar/goo/zoo.html"); // Disallowed
		final URI disallowedUri2 = URI.create("http://foo.bar/gaa.html"); // Disallowed
		final URI disallowedUri3 = URI.create("http://foo.bar/"); // Disallowed
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		var filter = URLRespectsRobots.parseRobotsResponse(fetchData, "any", crawlDelay);
		assertFalse(filter.isAllowed(disallowedUri1.toString()));
		assertFalse(filter.isAllowed(disallowedUri2.toString()));
		assertFalse(filter.isAllowed(disallowedUri3.toString()));
		assertTrue(crawlDelay.intValue() == 0);
	}

	@Test
	public void testAllowDisallowSync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bur/robots.txt");
		proxy.add200(robotsURL, "",
				"# goodguy can do anything\n" +
				"User-agent: goodguy\n" +
				"Disallow: \n\n" +
				"# badguy can do nothing\n" +
				"User-agent: badguy\n" +
				"Disallow: /\n"
		);

		final URI url = URI.create("http://foo.bur/goo/zoo.html"); // Disallowed

		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		assertTrue(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay), url));
		assertTrue(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy foo", crawlDelay), url));
		assertFalse(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy", crawlDelay), url));
		assertFalse(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy foo", crawlDelay), url));
	}

	@Test
	public void testAllowOnlySync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bor/robots.txt");
		proxy.add200(robotsURL, "",
				"# goodguy can do anything\n" +
				"User-agent: goodguy\n" +
				"Disallow:\n\n" +
				"# every other guy can do nothing\n" +
				"User-agent: *\n" +
				"Disallow: /\n"
		);
		final URI url = URI.create("http://foo.bor/goo/zoo.html"); // Disallowed
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		assertTrue(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay), url));
		assertTrue(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy foo", crawlDelay), url));
		assertFalse(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy", crawlDelay), url));
		assertFalse(URLRespectsRobots.apply(URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy foo", crawlDelay), url));
	}

	@Test
	public void test4xxSync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bor/robots.txt");
		proxy.addNon200(robotsURL, "HTTP/1.1 404 Not Found\n",
				"# goodguy can do anything\n" +
				"User-agent: goodguy\n" +
				"Disallow:\n\n" +
				"# every other guy can do nothing\n" +
				"User-agent: *\n" +
				"Disallow: /\n"
		);
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		assertEquals(0, URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay).disallow.length);
		assertEquals(0, URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy foo", crawlDelay).disallow.length);
	}


	@Test
	public void testComplexSync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bor/robots.txt");
		proxy.add200(robotsURL, "",
				"# every other guy can do nothing\n" +
				"User-agent: *\n" +
				"Crawl-delay: 10\n" +
				"Disallow: /y\n" +
				"Disallow: /a\n" +
				"Disallow: /c/d\n" +
				"Disallow: /e\n"
		);
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		final URLRespectsRobots.RobotsRules filter = URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay);
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/d")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/d")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/e")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/@")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/x")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/z")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a/b")));
		assertTrue(crawlDelay.intValue() == 10);
	}

	@Test
	public void testComplexSyncAllow() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bor/robots.txt");
		proxy.add200(robotsURL, "",
			"# every other guy can do nothing\n" +
				"User-agent: *\n" +
				"Crawl-delay: 10\n" +
				"Disallow: /y\n" +
				"Disallow: /a\n" +
				"Allow: /a/c\n" +
				"Disallow: /c/d\n" +
				"Disallow: /e\n"
		);
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		final URLRespectsRobots.RobotsRules filter = URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay);
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/d")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/d")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/e")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/@")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/x")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/z")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a/b")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a/c")));

		assertTrue(crawlDelay.intValue() == 10);
	}

	@Test
	public void testComplexSyncNoSpace() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bor/robots.txt");
		proxy.add200(robotsURL, "",
			"# every other guy can do nothing\n" +
				"User-agent:*\n" +
				"Crawl-delay:10\n" +
				"Disallow:/y\n" +
				"Disallow:/a\n" +
				"Disallow:/c/d\n" +
				"Disallow:/e\n"
		);
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		final URLRespectsRobots.RobotsRules filter = URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay);
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/d")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/d")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/c/e")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/@")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/x")));
		assertTrue(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/z")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a")));
		assertFalse(URLRespectsRobots.apply(filter, URI.create("http://foo.bor/a/b")));
		assertTrue(crawlDelay.intValue() == 10);
	}

	@Test
	public void testRedirectSync() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL0 = URI.create("http://foo.bar/robots.txt");
		URI robotsURL1 = URI.create("http://foo.bar/fubar/robots.txt");

		proxy.addNon200(robotsURL0, "HTTP/1.1 301 Moved Permanently\nLocation: " + robotsURL1 + "\n", "");
		proxy.add200(robotsURL1, "",
				"# goodguy can do anything\n" +
				"User-agent: goodguy\n" +
				"Disallow:\n\n" +
				"# every other guy can do nothing\n" +
				"User-agent: *\n" +
				"Disallow: /\n"
		);
		URI url = URI.create("http://foo.bar/goo/zoo.html"); // Disallowed
		proxy.add200(url, "", "Should not be crawled...");

		proxy.addNon200(URI.create("http://too.many/robots.txt"), "HTTP/1.1 301 Moved Permanently\nLocation: http://too.many/0\n", "");
		for(int i = 0; i < 5; i++) proxy.addNon200(URI.create("http://too.many/" + i), "HTTP/1.1 301 Moved Permanently\nLocation: http://too.many/" + (i + 1) + "\n", "");

		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), true);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		MutableInt crawlDelay = new MutableInt(0);
		fetchData.fetch(URI.create(BURL.schemeAndAuthority(url) + "/robots.txt"), null, httpClient, null, null, true);
		URLRespectsRobots.RobotsRules filter = URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy", crawlDelay);
		assertTrue(URLRespectsRobots.apply(filter, url));
		filter = URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy", crawlDelay);
		assertFalse(URLRespectsRobots.apply(filter, url));

		filter = URLRespectsRobots.parseRobotsResponse(fetchData, "goodGuy foo", crawlDelay);
		assertTrue(URLRespectsRobots.apply(filter, url));
		filter = URLRespectsRobots.parseRobotsResponse(fetchData, "badGuy foo", crawlDelay);
		assertFalse(URLRespectsRobots.apply(filter, url));

		fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(URI.create("http://too.many/robots.txt"), null, httpClient, null, null, true);
		assertTrue(fetchData.exception instanceof RedirectException);

		fetchData.close();
	}


	public void testLiebert() throws IOException {
		URLRespectsRobots.RobotsRules robots;
		MutableInt crawlDelay = new MutableInt(0);
		robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /\n\nUser-agent: Googlebot\nDisallow: /action\n"), "BUbiNG", crawlDelay);
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://online.liebertpub.com/doi/abs/10.1089/dna.2012.1756")));

		robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /\nDisallow: /action\n"), "BUbiNG", crawlDelay);
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://online.liebertpub.com/doi/abs/10.1089/dna.2012.1756")));
	}

	@Test
	public void testLiebert2() throws IOException {
		MutableInt crawlDelay = new MutableInt(0);
		URLRespectsRobots.RobotsRules robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /\n\nUser-agent: Googlebot\nDisallow: /action\n"), "BUbiNG", crawlDelay);
		assertEquals(1, robots.disallow.length);
		assertEquals("/", new String(robots.disallow[0]));
	}

	@Test
	public void testPrefixes() throws IOException {
		URLRespectsRobots.RobotsRules robots;
		MutableInt crawlDelay = new MutableInt(0);
		robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /a\nDisallow: /a/a\nDisallow: /a/a\n"), "BUbiNG", crawlDelay);
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/")));
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://example.com/a")));
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/b")));
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://example.com/a/b")));

		robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /a/b\nDisallow: /a\nDisallow: /b/c\nDisallow: /b\n"), "BUbiNG", crawlDelay);
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/c")));
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://example.com/a")));
		assertFalse(URLRespectsRobots.apply(robots, URI.create("http://example.com/b")));
	}

	@Test
	public void testDisallowStar() throws IOException{
		URLRespectsRobots.RobotsRules robots;
		MutableInt crawlDelay = new MutableInt(0);
		robots = URLRespectsRobots.parseRobotsReader(new StringReader("User-agent: *\nDisallow: /*buy_now*\n"), "BUbiNG", crawlDelay);
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/hi")));
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/hi_buy_now")));
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/buy_now_hi")));
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/buy_now/page")));
		assertTrue(URLRespectsRobots.apply(robots, URI.create("http://example.com/page/buy_now")));
	}


	@Test
	public void testPrefixesDeep() {
		Set<String> inset = new ObjectOpenHashSet<>();
		Set<String> pfset = new ObjectOpenHashSet<>();
		Random rand = new Random(0);
		for (int i = 100; i < 999; i++) {
			if (rand.nextDouble() < 0.3) {
				String commonPref = String.valueOf(i);
				boolean putPrefix = rand.nextDouble() < 0.9;
				if (putPrefix) {
					pfset.add(commonPref);
					inset.add(commonPref);
				}
				for (int j = 100; j < 450; j++) {
					if (rand.nextDouble() < 0.3) {
						inset.add(commonPref + j);
						if (! putPrefix) pfset.add(commonPref + j);
					}
				}
			}
		}
		char[][] resultArray = URLRespectsRobots.toSortedPrefixFreeCharArrays(inset);
		Set<String> result = new ObjectOpenHashSet<>();
		for (char[] a: resultArray) result.add(new String(a));
		assertEquals(result, pfset);


	}
	
	@Test
	public void testDisallowEverytingWithUTFBOM() throws Exception {
		proxy = new SimpleFixedHttpProxy();
		URI robotsURL = URI.create("http://foo.bar/robots.txt");
		proxy.add200(robotsURL, "",
				"\ufeff"+
						"User-agent: *\n" +
						"Disallow: /\n\n"
				);
		final URI disallowedUri1 = URI.create("http://foo.bar/goo/zoo.html"); // Disallowed
		final URI disallowedUri2 = URI.create("http://foo.bar/gaa.html"); // Disallowed
		final URI disallowedUri3 = URI.create("http://foo.bar/"); // Disallowed
		proxy.start();

		HttpClient httpClient = FetchDataTest.getHttpClient(new HttpHost("localhost", proxy.port()), false);

		FetchData fetchData = new FetchData(Helpers.getTestConfiguration(this));
		fetchData.fetch(robotsURL, null, httpClient, null, null, true);
		MutableInt crawlDelay = new MutableInt(0);
		URLRespectsRobots.RobotsRules filter = URLRespectsRobots.parseRobotsResponse(fetchData, "any", crawlDelay);
		assertFalse(URLRespectsRobots.apply(filter, disallowedUri1));
		assertFalse(URLRespectsRobots.apply(filter, disallowedUri2));
		assertFalse(URLRespectsRobots.apply(filter, disallowedUri3));
	}

}
