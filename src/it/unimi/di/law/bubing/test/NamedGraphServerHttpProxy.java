package it.unimi.di.law.bubing.test;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.*;
import it.unimi.di.law.bubing.util.ByteArrayCharSequence;
import it.unimi.di.law.bubing.util.MurmurHash3;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.StringMap;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ImmutableGraph;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Copyright (C) 2010-2017 Paolo Boldi, Massimo Santini, and Sebastiano Vigna
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

/** An HTTP proxy that uses a {@link NamedGraphServer} to generate fake HTML pages contaning only links. */

public class NamedGraphServerHttpProxy extends Thread {

	private static AtomicLong busyTime = new AtomicLong();
	private static AtomicLong simultaneousQueries = new AtomicLong();
	private static AtomicLong maxSimultaneousQueries = new AtomicLong();

	/** Like a standard print writer, but it sleeps a random amount of time before printing each string (only the
	 *  methods {@link #println(String)}, {@link #print(String)} and {@link #println()} are affected). 	*/
	public static class CapriciousPrintWriter extends PrintWriter {
		private final XoRoShiRo128PlusRandom random;
		private final long averageDelay, delayDeviation;

		public CapriciousPrintWriter(final Writer writer, final long averageDelay, final long delayDeviation, final XoRoShiRo128PlusRandom random) {
			super(writer);
			this.averageDelay = averageDelay;
			this.delayDeviation = delayDeviation;
			this.random = random;
		}

		private void sleep() {
			final long delay = Math.abs((long)(random.nextGaussian() * delayDeviation + averageDelay));
			try {
				Thread.sleep(delay);
			}
			catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		@Override
		public void println(String s) {
			sleep();
			super.println(s);
			super.flush();
		}
		@Override
		public void print(String s) {
			sleep();
			super.print(s);
			super.flush();
		}
		@Override
		public void println() {
			sleep();
			super.println();
			super.flush();
		}
	}

	public static void generate(final long hashCode, final StringBuilder content, final CharSequence[] successors, boolean notescurl) {
		content.append("<!DOCTYPE html>");
		content.append("<html>\n<head>");
		content.append("<!-- a very important comment -->");
		ThreadLocalRandom trng = ThreadLocalRandom.current();
		content.append("<title>My page ");
		content.append(RandomStringUtils.randomAlphabetic(trng.nextInt(10) + 4));
		content.append("</title>");

		content.append("<script type=\"text/javascript\">\n");
		content.append("if(self!==top)window.document.write(\"\\u003Cstyle>body * {display:none !important;}\\u003C\\/style>\\u003Ca href=\\\"#\\\" onclick=\"+\n" +
				"\"\\\"top.location.href=window.location.href\\\" style=\\\"display:block !important;padding:10px\\\">Go to Slack.com\\u003C\\/a>\");\n");
		content.append("</script>");

		if (trng.nextDouble() > 0.05) { // 95% of the time
			if (trng.nextDouble() > 0.8) // 30% of the time
				content.append("<meta charset=\"UTF-8\">\n");
			else
			if (trng.nextDouble() > 0.8) // 30% of the time
				content.append("<meta \ncharset=\"UTF-8\">\n");
			else
			if (trng.nextDouble() > 0.8) // 30% of the time
				content.append("<meta \nhttp-equiv=\"content-type\" \ncontent=\"text/html; charset=utf-8\">\n");
			else
				content.append("<meta http-equiv=\"content-type\" content=\"text/html; charset=utf-8\">\n");
            if (trng.nextDouble() > 0.8) // 30% of the time
                content.append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
		}

		content.append("</head>\n<body>\n");
		// This helps in making the page text different even for the same number
		// of URLs, but not always.
		content.append("<h1>").append((char)((hashCode & 0xF) + 'A')).append((char)((hashCode >>> 4 & 0xF) + 'A')).append((char)((hashCode >>> 8 & 0xF) + 'A')).append((char)((hashCode >>> 12 & 0xF) + 'A')).append("</h1>\n");
		String[] sentences = new String[] {
			"<p>Alan Mathison Turing OBE FRS (/ˈtjʊərɪŋ/; 23 June 1912 – 7 June 1954) was an English mathematician, computer scientist, logician, cryptanalyst, philosopher and theoretical biologist.[6] Turing was highly influential in the development of theoretical computer science, providing a formalisation of the concepts of algorithm and computation with the Turing machine, which can be considered a model of a general-purpose computer.[7][8][9] Turing is widely considered to be the father of theoretical computer science and artificial intelligence.[10] Despite these accomplishments, he was never fully recognized in his home country during his lifetime due to his homosexuality, which was then a crime in the UK.</p>\n",
			"<p>During the Second World War, Turing worked for the Government Code and Cypher School (GC&CS) at Bletchley Park, Britain's codebreaking centre that produced Ultra intelligence. For a time he led Hut 8, the section that was responsible for German naval cryptanalysis. Here he devised a number of techniques for speeding the breaking of German ciphers, including improvements to the pre-war Polish bombe method, an electromechanical machine that could find settings for the Enigma machine. Turing played a pivotal role in cracking intercepted coded messages that enabled the Allies to defeat the Nazis in many crucial engagements, including the Battle of the Atlantic, and in so doing helped win the war.[11][12] Counterfactual history is difficult with respect to the effect Ultra intelligence had on the length of the war,[13] but at the upper end it has been estimated that this work shortened the war in Europe by more than two years and saved over 14 million lives.[11]</p>\n",
			"<p>Turing was prosecuted in 1952 for homosexual acts, when by the Labouchere Amendment, \"gross indecency\" was a criminal offence in the UK. He accepted chemical castration treatment, with DES, as an alternative to prison. Turing died in 1954, 16 days before his 42nd birthday, from cyanide poisoning. An inquest determined his death as suicide, but it has been noted that the known evidence is also consistent with accidental poisoning.[15] In 2009, following an Internet campaign, British Prime Minister Gordon Brown made an official public apology on behalf of the British government for \"the appalling way he was treated\". Queen Elizabeth II granted him a posthumous pardon in 2013.[16][17][18] The Alan Turing law is now an informal term for a 2017 law in the United Kingdom that retroactively pardoned men cautioned or convicted under historical legislation that outlawed homosexual acts.[19]</p>\n",
			"<p>Alan Mathison Turing, né le 23 juin 1912 à Londres et mort le 7 juin 1954 à Wilmslow, est un mathématicien et cryptologue britannique, auteur de travaux qui fondent scientifiquement l'informatique. Pour résoudre le problème fondamental de la décidabilité en arithmétique, il présente en 1936 une expérience de pensée que l'on nommera ensuite machine de Turing et des concepts de programme et de programmation, qui prendront tout leur sens avec la diffusion des ordinateurs, dans la seconde moitié du xxe siècle. Son modèle a contribué à établir la thèse de Church, qui définit le concept mathématique intuitif de fonction calculable.</p>\n",
			"<p>Durant la Seconde Guerre mondiale, il joue un rôle majeur dans la cryptanalyse de la machine Enigma utilisée par les armées allemandes. Ce travail secret ne sera connu du public que dans les années 1970. Après la guerre, il travaille sur un des tout premiers ordinateurs, puis contribue au débat sur la possibilité de l'intelligence artificielle, en proposant le test de Turing. Vers la fin de sa vie, il s'intéresse à des modèles de morphogenèse du vivant conduisant aux « structures de Turing ». Poursuivi en justice en 1952 pour homosexualité, il choisit pour éviter la prison la castration chimique par prise d'œstrogènes. Il est retrouvé mort par empoisonnement au cyanure le 7 juin 1954 dans la chambre de sa maison à Wilmslow. La reine Élisabeth II le reconnaît comme héros de guerre et le gracie à titre posthume en 2013.</p>\n",
			"<p>À cause de son manque d'enthousiasme à travailler autant dans les matières classiques que dans les matières scientifiques, Turing échoue plusieurs fois à ses examens. Il n'est admis qu'au King's College de l'université de Cambridge, alors qu'il avait demandé Trinity College en premier choix. Il étudie de 1931 à 1934 sous la direction de Godfrey Harold Hardy, mathématicien alors titulaire de la chaire sadleirienne puis responsable du centre de recherches et d'études en mathématiques. Il suit également les cours d'Arthur Eddington et, la dernière année, de Max Newman qui l'initie à la logique mathématique. En 1935, Turing est élu fellow du King's College, l'équivalent d'une bourse de thèse, grâce à sa démonstration du théorème central limite8.</p>\n",
			"<p>Alan Mathison Turing OBE,[2] FRS[3] [ˈælən ˈmæθɪsən ˈtjʊəɹɪŋ] (* 23. Juni 1912 in London; † 7. Juni 1954 in Wilmslow, Cheshire) war ein britischer Logiker, Mathematiker, Kryptoanalytiker und Informatiker. Er gilt heute als einer der einflussreichsten Theoretiker der frühen Computerentwicklung und Informatik. Turing schuf einen großen Teil der theoretischen Grundlagen für die moderne Informations- und Computertechnologie. Als richtungsweisend erwiesen sich auch seine Beiträge zur theoretischen Biologie.Das von ihm entwickelte Berechenbarkeitsmodell der Turingmaschine bildet eines der Fundamente der Theoretischen Informatik. Während des Zweiten Weltkrieges war er maßgeblich an der Entzifferung der mit der „Enigma“ verschlüsselten deutschen Funksprüche beteiligt. Der Großteil seiner Arbeiten blieb auch nach Kriegsende unter Verschluss.</p>\n",
			"<p>Turing entwickelte 1953 eines der ersten Schachprogramme, dessen Berechnungen er mangels Hardware selbst durchführte. Nach ihm benannt sind der Turing Award, die bedeutendste Auszeichnung in der Informatik, sowie der Turing-Test zum Überprüfen des Vorhandenseins von künstlicher Intelligenz.</p>\n",
			"<p>Im März 1952 wurde Turing wegen seiner Homosexualität, die damals noch als Straftat verfolgt wurde, zur chemischen Kastration verurteilt.[5] Turing erkrankte in Folge der Hormonbehandlung an einer Depression und starb etwa zwei Jahre später durch Suizid. Im Jahr 2009 sprach der damalige britische Premierminister Gordon Brown eine offizielle Entschuldigung im Namen der Regierung für die „entsetzliche Behandlung“ Turings aus und würdigte dessen „außerordentliche Verdienste“ während des Krieges; eine Begnadigung wurde aber noch 2011 trotz einer Petition abgelehnt. Am Weihnachtstag, dem 24. Dezember 2013 sprach Königin Elisabeth II. posthum ein „Royal Pardon“ (Königliche Begnadigung) aus.</p>\n",
			"<p>Lorem ipsum dolor sit amet , consectetur adipisici elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.</p>\n"
		};
		int nbLang = 4;
		int lang = Math.abs(trng.nextInt()) % nbLang;

		for (final CharSequence s : successors) {
			String ref = s.toString();
			if (!notescurl) ref = StringEscapeUtils.escapeHtml(s.toString());

			int index = Math.min(lang * 3 + Math.abs(trng.nextInt()) % 3, sentences.length - 1);
			content.append(sentences[index] + " <a href=\"" + ref + "\">" + ref + "</a> ");
			/*content.append("<p>Lorem ipsum dolor sit amet <a href=\""
					+ ref
					+ "\">"
					+ ref
					+ "</a>, consectetur adipisici elit, sed eiusmod tempor incidunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\n");*/
		}
		content.append("</body>\n</html>\n");
	}

	private static void generateUnique(final long hashCode, final StringBuilder content, final CharSequence[] successors, boolean notescurl){
		//TODO: improve the following
		content.append("<h1>");
		content.append((""+ hashCode).replace('0', 'g').replace('1', 'h').replace('2', 'i').replace('3', 'l').replace('4', 'm').replace('5', 'n').replace('6', 'o').replace('7', 'p').replace('8', 'q').replace('9', 'r'));
		content.append("</h1>\n");
		content.append("<h1>").append(hashCode).append("</h1>\n");
		generate(hashCode, content, successors, notescurl);
	}

	/** Estimates the length of the page generated by a given array of successors.
	 *
	 * @param successors the array of successors, or {@code null}.
	 * @return an estimate of the length of the page returned by generation methods.
	 */
	public final static int estimateLength(final CharSequence[] successors) {
		return 600 * (successors == null ? 1 : successors.length + 1);
	}

	private static class Task implements Runnable {

		private Socket socket;
		private final NamedGraphServer graphServer;
		private final double frac404;
		private final double frac500;
		private final XoRoShiRo128PlusRandom frac500Random;
		private final double fracDelayed;
		private final long averageDelay;
		private final long delayDeviation;
		private final Semaphore stuckInCapriciousWriting;
		private final int generalPageSpeed;
		private final boolean uniquify;
		private final boolean notescurl;

		public Task(final NamedGraphServer graphServer, final Semaphore stuckInCapriciousWriting, final int generalPageSpeed, final double frac404, final double frac500, final double fracDelayed,
				final long averageDelay, final long delayDeviation, boolean undigitize, boolean notescurl) {
			this.graphServer = graphServer;
			this.stuckInCapriciousWriting = stuckInCapriciousWriting;
			this.generalPageSpeed = generalPageSpeed;
			this.frac404 = frac404;
			this.frac500 = frac500;
			this.frac500Random = new XoRoShiRo128PlusRandom();
			this.fracDelayed = fracDelayed;
			this.averageDelay = averageDelay;
			this.delayDeviation = delayDeviation;
			this.uniquify = undigitize;
			this.notescurl = notescurl;
		}

		public void setSocket(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				long currentSimultaneous = simultaneousQueries.incrementAndGet();
				maxSimultaneousQueries.updateAndGet(value -> value < currentSimultaneous ? currentSimultaneous : value);
				final long startTime = System.nanoTime();
				final FastBufferedInputStream in = new FastBufferedInputStream(socket.getInputStream());
				final byte[] array = new byte[4096];

				final int len = in.readLine(array);
				int first = -1, second = -1;

				for (int i = 0; i < len; i++) {
					if (array[i] == ' ') {
						if (first == -1) {
							first = i;
						} else {
							second = i;
							break;
						}
					}
				}

				if (frac500Random.nextDouble() > 1 - frac500 || first == -1 || second == -1) {
					final PrintWriter out = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(socket.getOutputStream()), Charsets.ISO_8859_1));
					out.println("HTTP/1.1 500 Server error");
					out.println();
					out.close();
				} else {
					final ByteArrayCharSequence name = new ByteArrayCharSequence(array, first + 1, second - first - 1);

					final int hashCode = name.hashCode();
					final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(hashCode);

					// We become capricious if a toss coin suggests it *and* there are not too many threads doing it.
					final PrintWriter out = random.nextDouble() < fracDelayed && stuckInCapriciousWriting.tryAcquire() ?
							new CapriciousPrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(socket.getOutputStream()), Charsets.ISO_8859_1), averageDelay, delayDeviation, random) :
							new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(socket.getOutputStream()), Charsets.ISO_8859_1));

					try {
						//System.out.println("PROCESSING: " + name + " with String " + new MutableString(name).toString());
						final CharSequence[] successors = graphServer.successors(name);
						final MutableString currentName = new MutableString(name);

						while (in.readLine(array) > 0)
							; // Note that name is based on array, but we don't use it anymore.

						final StringBuilder content = new StringBuilder(estimateLength(successors));

						if (successors == null || random.nextDouble() < frac404) {
							content.append("<html>\n");
							content.append("<head></head>\n");
							content.append("<body>\n");
							content.append("<h1>404 Not found</h1>\n");
							content.append("</body>\n");
							content.append("</html>\n");

							out.println("HTTP/1.1 404 Not found");
							out.println("Connection: close");
							out.println("Content-Type: text/html; charset=iso-8859-1");
							out.println("Content-Length: " + content.length());
							out.println();

							//System.err.println("404: pred:"  + (600 * (successors == null ? 1 : successors.length + 1))  + " real: " + content.length());
							out.println(content.toString());
							out.println();
						} else {
							if (uniquify)
								generateUnique(MurmurHash3.hash(currentName.toString().getBytes()), content, successors, notescurl);
							else generate(hashCode, content, successors, notescurl);
							out.println("HTTP/1.1 200 OK");
							out.println("Connection: close");
							ThreadLocalRandom trng = ThreadLocalRandom.current();
							if ( trng.nextDouble() > 0.8 )
								out.println("Content-Type: text/html; charset=iso-8859-1");
							else
								out.println("Content-Type: text/html");
							out.println("Content-Length: " + content.length());
							out.println();
							//System.err.println("200: pred:"  + (600 * (successors == null ? 1 : successors.length + 1))  + " real: " + content.length());
							out.println(content.toString());
							out.println();
						}
						try {
							Thread.sleep((long) (1000.0 * content.length() / generalPageSpeed));
						} catch (final InterruptedException cantHappen) {
						}

						out.close();
					} finally {
						if (out instanceof CapriciousPrintWriter) stuckInCapriciousWriting.release();
					}
				}

				in.close();
				socket.close();
				busyTime.addAndGet(System.nanoTime() - startTime);
			} catch (final IOException e) {
				e.printStackTrace();
			} finally {
				simultaneousQueries.decrementAndGet();
			}
		}
	}

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] arg) throws IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(NamedGraphServerHttpProxy.class.getName(), "Starts a HTTP Proxy Server for a given named immutable graph.",
				new Parameter[] {
					new FlaggedOption("sites", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "sites", "The number of sites for a random server."),
					new FlaggedOption("degree", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "degree", "The (out)degree of for a random server."),
					new FlaggedOption("maxDepth", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'm', "max-depth", "The maximum depth of a site for a random server."),
					new FlaggedOption("frac404", JSAP.DOUBLE_PARSER, "0", JSAP.NOT_REQUIRED, '4', "frac-404", "The fraction of 404 pages."),
					new FlaggedOption("frac500", JSAP.DOUBLE_PARSER, "0", JSAP.NOT_REQUIRED, '5', "frac-500", "The fraction of 505 pages."),
					new FlaggedOption("fracDelayed", JSAP.DOUBLE_PARSER, "0", JSAP.NOT_REQUIRED, 'D', "frac-delayed", "The fraction of pages that exhibit delays in the connection."),
					new FlaggedOption("averageDelay", JSAP.LONGSIZE_PARSER, "1000", JSAP.NOT_REQUIRED, 'A', "average-delay", "The average delay (in ms) per line in a delayed connection."),
					new FlaggedOption("generalPageSpeed", JSAP.INTEGER_PARSER, Integer.toString(Integer.MAX_VALUE), JSAP.NOT_REQUIRED, 'g', "general-page-speed", "The page speed in bytes per second."),
					new FlaggedOption("delayDeviation", JSAP.LONGSIZE_PARSER, "1000", JSAP.NOT_REQUIRED, 'S', "delay-deviation", "The standard deviation of delays (in ms) per line in a delayed connection."),
					new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "1", JSAP.NOT_REQUIRED, 't', "threads", "The number of threads for a multithreaded server."),
					new FlaggedOption("port", JSAP.INTEGER_PARSER, Integer.toString(8080), JSAP.NOT_REQUIRED, 'p', "port", "The proxy port."),
					new FlaggedOption("seed", JSAP.LONG_PARSER, "0", JSAP.NOT_REQUIRED, 'r', "seed", "The seed used to decide delays."),
					new Switch("uniquify", 'u', "uniquify", "Append at the beginning of each page a string representation of the hashcode of the URL (to avoid duplicated pages when all links differ only for the presence of digits)."),
					new Switch("notescurl", 'e', "notescurl", "If true the URLs in the produced page are not escaped"),
					new Switch("padding", 'h', "padding", "If true the URLs are not padded, meaning that each host is of the type x.x.x.x, where x starts with a number (just for random server)"),
					new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of graph (- for a random server)."),
					new UnflaggedOption("map", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The string map for a graph-based server.")
				}
			);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final int port = jsapResult.getInt("port");
		if (port <= 0 || port > 65536) {
			System.err.println("Port value must be in (0, 65535].");
			System.exit(1);
		}

		final String graphBasename = jsapResult.getString("graph");

		ServerSocket serverSocket = null;

		NamedGraphServer graphServer = null;
		if ("-".equals(graphBasename)) {
			if (! jsapResult.userSpecified("sites")) throw new IllegalArgumentException("You must specify the number of sites of the random server.");
			if (! jsapResult.userSpecified("degree")) throw new IllegalArgumentException("You must specify the degree of the random server.");
			if (! jsapResult.userSpecified("maxDepth")) throw new IllegalArgumentException("You must specify the maximum depth of the random server.");
			if (jsapResult.userSpecified("map")) throw new IllegalArgumentException("You must specify the map only for a graph-based server.");
			graphServer = new RandomNamedGraphServer(jsapResult.getInt("sites"), jsapResult.getInt("degree"), jsapResult.getInt("maxDepth"), jsapResult.getBoolean("padding"));
		}
		else {
			if (jsapResult.getBoolean("padding")) throw new IllegalArgumentException("You can specify the padding option only for a random server.");
			if (jsapResult.userSpecified("sites")) throw new IllegalArgumentException("You must specify the number of sites only for a random server.");
			if (jsapResult.userSpecified("degree")) throw new IllegalArgumentException("You must specify the degree only for a random server.");
			if (jsapResult.userSpecified("maxDepth")) throw new IllegalArgumentException("You must specify the maximum depth only for a random server.");
			if (! jsapResult.userSpecified("map")) throw new IllegalArgumentException("You must specify the map of a graph-based server.");
			final ImmutableGraph graph = ImmutableGraph.load(graphBasename);
			final StringMap<MutableString> map = (StringMap<MutableString>)BinIO.loadObject(jsapResult.getString("map"));
			graphServer = new ImmutableGraphNamedGraphServer(graph, map);
		}

		final int numThreads = jsapResult.getInt("threads");

		try {
			serverSocket = new ServerSocket(port,500);
			System.err.println("Started on: " + port);
		} catch (final IOException e) {
			System.err.println("Could not listen on port: " + port);
			System.exit(1);
			return; // To avoid null pointer flagging
		}
		final Executor exec = Executors.newFixedThreadPool(numThreads);
		final Semaphore stuckInCapriciousWriting = new Semaphore(Math.max(numThreads / 10, 1));

		long lastReportTime = System.currentTimeMillis(), lastBusyTime = 0;
		final long startTime = lastReportTime;
		int generalPageSpeed = jsapResult.getInt("generalPageSpeed");
		double frac404 = jsapResult.getDouble("frac404");
		double frac500 = jsapResult.getDouble("frac500");
		double fracDelayed = jsapResult.getDouble("fracDelayed");
		long averageDelay = jsapResult.getLong("averageDelay");
		long delayDeviation = jsapResult.getLong("delayDeviation");
		boolean uniquify = jsapResult.getBoolean("uniquify");
		boolean notescurl = jsapResult.getBoolean("notescurl");

		for (long i = 0;; i++) {
			final Task task = new Task(graphServer.copy(), stuckInCapriciousWriting, generalPageSpeed, frac404, frac500,
					fracDelayed, averageDelay, delayDeviation, uniquify, notescurl);
			task.setSocket(serverSocket.accept());
			exec.execute(task);

			if ((i & 0xFFFF) != 0) {
				final long currentTime = System.currentTimeMillis();
				if (currentTime - lastReportTime > 10000) {
					final long currentBusyTime = busyTime.get();
					System.err.println(Util.format(currentBusyTime / 10000.0 / numThreads / (currentTime - startTime)) + "% [" + Util.format((currentBusyTime - lastBusyTime) / 10000.0 / numThreads / (currentTime - lastReportTime)) + "%]");
					System.err.println("Max simultaneaous queries : " + maxSimultaneousQueries.getAndSet(0));
					lastReportTime = currentTime;
					lastBusyTime = currentBusyTime;
				}
			}
		}
	}
}
