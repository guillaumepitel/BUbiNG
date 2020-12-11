package it.unimi.di.law.warc.filters;

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

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.net.URI;
import java.util.Arrays;
import java.util.Set;

// RELEASE-STATUS: DIST

/** A filter accepting only URIs whose path ends (case-insensitively) with one of a given set of suffixes. */
public class URLContainsOneOf extends AbstractFilter<URI> {

	/** The splitter used to parse a set of comma separated extensions in an arraylist */
	private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

	/** The accepted parts, downcased. */
	private final String[] parts;

	/** Creates a filter that only accepts URLs whose path contains one of a given set of strings.
	 *
	 * @param parts the accepted suffixes.
	 */
	public URLContainsOneOf(final String[] parts) {
		this.parts = new String[parts.length];
		for (int i = 0; i < parts.length; i++) this.parts[i] = parts[i].toLowerCase();
	}

	/**
	 * Apply the filter to a given URI
	 *
	 * @param uri the URI to be filtered
	 * @return <code>true</code> if <code>uri</code> ends with one of the accepted parts
	 */
	@Override
	public boolean apply(final URI uri) {
		String file = uri.getRawPath().toLowerCase();
		for (String part: parts) if (file.contains(part)) return true;
		return false;
	}

	/**
	 * Get a new <code>URLContainsOneOf</code> that will accept only URIs containing one of the allowed parts
	 *
	 * @param spec a string containing the allowed suffixes (separated by ',')
	 * @return a new <code>PathEndsWithOneOf</code> that will accept only URIs whose path suffix is one of the strings specified by <code>spec</code>
	 */
	public static URLContainsOneOf valueOf(String spec) {
		return new URLContainsOneOf(Iterables.toArray(SPLITTER.split(spec), String.class));
	}

	/**
	 * A string representation of the state of this object, that is just the suffixes allowed.
	 *
	 * @return the strings used by this object to compare suffixes
	 */
	@Override
	public String toString() {
		return toString((Object[])parts);
	}

	/**
	 * Compare this with a given generic object
	 *
	 * @return <code>true</code> if <code>x</code> is an instance of <code>HostEndsWithOneOf</code> and the suffixes allowed by <code>x</code> are allowed by this and vice versa
	 */
	@Override
	public boolean equals(Object x) {
		if (x instanceof URLContainsOneOf) {
			Set<String> suffixSet = new ObjectOpenHashSet<>(parts);
			Set<String> xSuffixSet = new ObjectOpenHashSet<>(((URLContainsOneOf)x).parts);
			return suffixSet.equals(xSuffixSet);
		}
		else return false;
	}


	@Override
	public int hashCode() {
		return Arrays.hashCode(parts) ^ URLContainsOneOf.class.hashCode();
	}

	@Override
	public Filter<URI> copy() {
		return this;
	}
}
