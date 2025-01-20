package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class Crawler {

    private static final Pattern URL_PATTERN = Pattern.compile("href {0,20}= {0,20}[\"']?([^'\">]{0,150})['\"]?[^>]{0,50}>([^<]{0,100})< {0,20}/a {0,20}>");

    private static int MAX_QUEUE_SIZE = 30000;

    private static final Logger logger = Logger.getLogger(Crawler.class);
    private static final String[] badExtensions = {"mng", "pct", "bmp", "gif", "jpg", "jpeg", "png", "pst", "psp", "tif",
            "tiff", "ai", "drw", "dxf", "eps", "ps", "svg", "mp3", "wma", "ogg", "wav", "ra", "aac", "mid", "au", "aiff", "3gp", "asf", "asx", "avi", "mov", "mp4", "mpg", "qt", "rm", "swf", "wmv",
            "m4a", "css", "pdf", "doc", "exe", "bin", "rss", "zip", "rar", "cn", "in", "ru", "de", "fr", "jp", "il", "tr", "pl", "nl"};

    /*
     * Returns a comma-separated string of the title words if title exists, otherwise returns null.
     * Title words are used for ranking.
     */
    public static String getTitleWords(String title) {
        // parse the title, remove punctuation, set words to lowercase, and
        // concatenate into a comma-separated string
        title = title.strip();
        Pattern p = Pattern.compile("[.,:;!?'\"()-]");
        Matcher m = p.matcher(title);
        while (m.find()) {
            title = title.replace(m.group(), " ");
        }

        title = title.replaceAll("[^\\x00-\\x7F]", "");

        String[] words = title.split(" ");
        String titleWords = "";
        for (String word : words) {
            if (!word.equals("")) {
                word = word.toLowerCase();
                // remove stop words from title words
                if (Stopwords.STOP_WORDS.contains(word)) {
                    continue;
                }
                titleWords += word + ",";
            }
        }
        return titleWords;
    }


    /* Returns page title if exists, otherwise returns null. */
    public static String getPageTitle(String page) {
        int titleTag = page.indexOf("<title>");
        int titleCloseTag = page.indexOf("</title>");

        if (titleTag != -1 && titleCloseTag != -1 && titleCloseTag > titleTag) {
            String title = page.substring(titleTag + 7, titleCloseTag);
            title = title.strip();

            // To avoid errors when splitting/concatenating the URL string in Indexer this
            // will remove commas, colons, and hyphens, then replace spaces with hyphens.
            // Ex) "Pizza: title-page for pizza.com!" -> "Pizza-title-page-for-pizza.com!"

            // first remove commas and colons
            Pattern p = Pattern.compile("[,:]");
            Matcher m = p.matcher(title);
            while (m.find()) {
                title = title.replace(m.group(), "");
            }
            // then replace hyphens with space
            p = Pattern.compile("[-]");
            m = p.matcher(title);
            while (m.find()) {
                title = title.replace(m.group(), " ");
            }
            // then replace all spaces with hyphens
            title = title.replace(" ", "-");

            title = title.replaceAll("[^\\x00-\\x7F]", "");

            return title;
        }
        return null;
    }


    /**
     * Removes HTML from page content for placing into table.
     * Currently removes any tags (HTML and script).
     * Trims whitespace.
     *
     * @param page, a messy page
     * @return cleaner page
     */
    public static String stripPageContent(String page) {
        String stylereg = "< *?style.*?>[\\s\\S]*?< *?/style.*?>";
        String scriptreg = "< *?script.*?>[\\s\\S]*?< *?/script.*?>";
        Pattern p = Pattern.compile(stylereg);
        Matcher m = p.matcher(page);
        while (m.find()) {
            page = page.replaceAll(stylereg, " ");
            m = p.matcher(page);
        }

        p = Pattern.compile(scriptreg);
        m = p.matcher(page);
        while (m.find()) {
            page = page.replaceAll(scriptreg, " ");
            m = p.matcher(page);
        }
        page = page.replaceAll("<[^<>]*>", " ");
        page = page.replaceAll("[\\s ]+", " ");
        return page;
    }

    /**
     * Uses page metadata to detect if a given page is in English.
     * Defaults to true if no lang property is set.
     *
     * @param page
     * @return true if English or no language metadata found
     */
    public static boolean isEnglish(String page) {
        int langTag = page.indexOf("lang=");
        if (langTag != -1) {
            int langCloseTag = page.substring(langTag).indexOf(">");
            if (langCloseTag != -1 && langCloseTag > langTag) {
                return page.substring(langTag, langCloseTag).contains("en");
            }
        }
        return true;


    }

    public static List<String> getAllowDisallowFromTable(KVSClient kvs, String host) throws IOException {
        String s = new String(kvs.get("pt-hosts", host, "allowDisallow"));
        s = s.replaceAll("[^\\x00-\\x7F]", "");
        return Arrays.asList(s.substring(1, s.length() - 1).split(", "));
    }

    /**
     * Reads in the seed urls from the given file.
     * File should be formatted with one url per line
     *
     * @param file
     * @return list of strings containing the seedURLS
     * @throws IOException
     */
    public static LinkedList<String> getSeedURLs(String file) throws IOException {
        LinkedList<String> seedURLs = new LinkedList<>();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String seed;
        while (((seed = br.readLine()) != null)) {
            seedURLs.add(normalizeURL(seed));
        }
        br.close();
        return seedURLs;
    }


    /**
     * Checks whether a url matches a blacklisted pattern (based on the command line argument)
     *
     * @param blacklistPatterns
     * @param url
     * @return
     */
    public static boolean isBlacklisted(Vector<String> blacklistPatterns, String url) {
        for (String pattern : blacklistPatterns) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(url);
            if (m.matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether a url is allowed based on the allowed/disallowed patterns from the site's
     * robots.txt file.
     *
     * @param allowDisallowList
     * @param url
     * @return
     */
    public static boolean isAllowedRobots(List<String> allowDisallowList, String url) {
        if (allowDisallowList == null || allowDisallowList.isEmpty()) {
            return true;
        }
        try {
            for (String allowDisallow : allowDisallowList) {
                String[] rule = allowDisallow.split(":");
                if (rule.length == 1) {
                    return true;
                }
                String allowOrDisallow = rule[0];
                String pattern = normalizePattern(rule[1]);


                Pattern p = Pattern.compile(pattern.trim());
                Matcher m = p.matcher(url);
                if (m.matches()) {
                    if (allowOrDisallow.equalsIgnoreCase("allow")) {
                        return true;
                    } else if (allowDisallow.equalsIgnoreCase("disallow")) {
                        return false;
                    }
                }
            }
        } catch(Exception e){
            return true;
            //logger.info("Bad pattern matching at: " + url + " with pattern " + pattern);
        }
        return true;
        }

    /**
     * Normalizes a string pattern into regex-friendly pattern
     *
     * @param pattern
     * @return
     */
    public static String normalizePattern(String pattern) {
        String normPattern = pattern.trim();
        normPattern = normPattern.replace(".", "\\.");
        normPattern = normPattern.replace("*", ".*");
        normPattern = normPattern.replace("/", "\\/");
        return normPattern;
    }

    /**
     * Parses a host's robots.txt file.
     * Builds the allowDisallow list and adds it to the hosts table.
     * Also sets crawl-delay, if provided.
     *
     * @param robots
     * @param kvs
     * @param host
     * @throws IOException
     */
    public static void parseRobots(String robots, KVSClient kvs, String host) throws IOException {
        //add the robots file to the table
        //probably not necessary but good for debugging
        //kvs.put("pt-hosts", host, "robots", robots);

        //robots = robots.replaceAll("[^\\x00-\\x7F]", "");

        //split into rules for different user agents
        String[] rules = robots.toLowerCase().split("user-agent:");

        //create the allowDisallow list
        ArrayList<String> adList = new ArrayList<>();
        BufferedReader br;
        for (String rule : rules) {
            rule = rule.trim();
            if (!rule.startsWith("*") && (!rule.startsWith("cis5550-crawler"))) {
                continue;
            }
            br = new BufferedReader(new StringReader(rule));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.split(":").length < 2) {
                    continue;
                }

                //if there is a crawlDelay line, set the delay
                if (line.startsWith("Crawl-delay")) {
                    kvs.put("pt-hosts", host, "crawl-delay", String.valueOf(Float.parseFloat(line.split(":")[1]) * 1000));
                }

                //populate the allow/disallow list
                if (line.startsWith("allow") || line.startsWith("disallow")) {
                    adList.add(line);
                }
            }
            br.close();
        }
        kvs.put("pt-hosts", host, "allowDisallow", adList.toString());
    }

    public static String normalizeURL(String currentURL) {
        return normalizeURL(currentURL, null);
    }

    public static String normalizeURL(String nextURL, String currentURL) {
        nextURL = nextURL.replaceAll("[^\\x00-\\x7F]", "");
        if (currentURL != null) {
            currentURL = currentURL.replaceAll("[^\\x00-\\x7F]", "");
        }

        int hash = nextURL.indexOf("#");
        if (hash != -1) {
            nextURL = nextURL.substring(0, hash);
        }

        String[] nextParts = URLParser.parseURL(nextURL);

        //fix port
        if (nextParts[2] == null) {
            if (nextParts[0] != null) {
                if (nextParts[0].equals("http")) {
                    nextParts[2] = "80";

                } else if (nextParts[0].equals("https")) {
                    nextParts[2] = "443";
                }
            } else {
                nextParts[0] = "https";
                nextParts[2] = "443";
            }
        }

        if (currentURL != null) {
            //normalize relative URLs
            String[] currentParts = URLParser.parseURL(currentURL);

            // add protocol
            if (nextParts[0] == null) {
                nextParts[0] = currentParts[0];
            }

            //add host
            if (nextParts[1] == null) {
                nextParts[1] = currentParts[1];
            }

            if (nextParts[2] == null) {
                if (nextParts[0].equals("http")) {
                    nextParts[2] = "80";

                } else if (nextParts[0].equals("https")) {
                    nextParts[2] = "443";
                }
            }

            //fix url
            if (nextParts[1].equals(currentParts[1])) {
                String base = currentParts[3];
                String newURL = nextParts[3];

                if (newURL.isEmpty()) {
                    nextParts[3] = base;
                } else if (!newURL.startsWith("/")) {
                    base = base.substring(0, base.lastIndexOf("/"));
                    while (newURL.contains("..")) {
                        newURL = newURL.substring(newURL.indexOf("..") + 2);
                        try {
                            base = base.substring(0, base.lastIndexOf("/"));
                        } catch (StringIndexOutOfBoundsException e) {
                          //  logger.info("You messed up the normalizer at: " + nextURL);
                        }
                    }

                    if (!newURL.startsWith("/")) {
                        newURL = "/" + newURL;
                    }
                    nextParts[3] = base + newURL;
                    if (nextParts[3].contains("?")) {
                        nextParts[3] = nextParts[3].substring(0, nextParts[3].indexOf("?"));
                    }
                }

            }
        }
        return nextParts[0] + "://" + (nextParts[1] == null ? nextParts[3] : nextParts[1]) + ":" + nextParts[2] + (nextParts[1] == null ? "/" : nextParts[3]);
    }

    /**
     * Checks a url against a BAD list of extensions.
     *
     * @param s
     * @param extensions
     * @return
     */
    public static boolean goodURLExtension(String s, String[] extensions) {
        for (String ext : extensions) {
            if (s.endsWith(ext)) {
                return false;
            }
        }
        return true;
    }

    public static HashSet<String> extractURLS(String page, String currentURL, KVSClient kvs, List<String> allowDisallowList) throws IOException {
        //long time = System.currentTimeMillis();
        //logger.info("Starting to extract urls from " + currentURL);
        HashSet<String> urls = new HashSet<>();
        Matcher matcher = URL_PATTERN.matcher(page);
        int count = 0;

        // parse current URL once
        String[] parsedCurrentURL = URLParser.parseURL(currentURL);
        String currentURLBase = parsedCurrentURL[1];

        while (matcher.find()) {
            //stop searching once we've found 15 urls
            //if they want them to count, then they can move'em up.
            if (count > 15) {
                break;
            }
            String anchorURL = matcher.group(1).trim();
            String anchorText = matcher.group(2).trim();

            // remove non-ASCII characters from anchorURL
            anchorURL = anchorURL.replaceAll("[^\\x00-\\x7F]", "");
            anchorText = anchorText.replaceAll("[^\\x00-\\x7F]", "");

            String normURL = normalizeURL(anchorURL, currentURL);
            String[] parsedURL = URLParser.parseURL(normURL);

            // skip inval or disallowed URLs
            if (parsedURL == null || (!parsedURL[0].equals("http") && !parsedURL[0].equals("https"))) continue;
            if (!goodURLExtension(parsedURL[3], badExtensions)) continue;
            if (getPageDepth(parsedURL[3]) > 4) {
               // logger.info("page not added because too deep: " + parsedURL[3]);
                continue;
            }
            //if (!isAllowedRobots(allowDisallowList, normURL)) continue;

            if (normURL.contains("?")) {
                normURL = normURL.substring(0, normURL.indexOf("?"));
            }

            // avoid dupes
            String hashedURL = Hasher.hash(normURL);
            if (kvs.existsRow("pt-crawl", hashedURL)) continue;

            urls.add(normURL);
            count++;

            // process anchor text for ext links
            boolean isExternalLink = parsedURL[1] != null && !parsedURL[1].equals(currentURLBase);
            if (isExternalLink && !anchorText.isEmpty()) {
                StringBuilder anchorWordsBuilder = new StringBuilder();
                for (String word : anchorText.replaceAll("[:,\\-]", " ").split(" ")) {
                    word = word.toLowerCase().strip();

                    word = word.replaceAll("[^\\x00-\\x7F]", "");

                    if (!word.isEmpty() && !Stopwords.STOP_WORDS.contains(word)) {
                        anchorWordsBuilder.append(word).append(",");
                    }
                }


                // update anchor words in KVS
                String key = Hasher.hash(normURL);
                String newAnchorWords = anchorWordsBuilder.toString();
                if (kvs.existsRow("pt-anchors", key)) {
                    String currentAnchorWords = new String(kvs.get("pt-anchors", key, "anchorWords"));
                    newAnchorWords += currentAnchorWords;
                    Set<String> anchorWordSet = new HashSet<>(Arrays.asList(newAnchorWords.split(",")));
                    newAnchorWords = String.join(",", anchorWordSet) + ",";
                }
                kvs.put("pt-anchors", key, "anchorWords", newAnchorWords);
            }
        }
        //logger.info("Time to extract from " + currentURL + " : " + (System.currentTimeMillis() - time));
        return urls;
    }

    static int getPageDepth(String page) {
        int count = 0;
        for (char c : page.toCharArray()) {
            if (c == '/') {
                count++;
            }
        }
        return count;
    }


    public static void run(FlameContext ctx, String[] args) throws Exception {
        if (!(args.length == 1 || args.length == 2)) {
            ctx.output("Error: Incorrect arguments");
            return;
        } else {
            ctx.output("OK");
        }

        // parse blacklist patterns
        Vector<String> blacklistPatterns = new Vector<>();
        if (args.length == 2) {
            String blacklistTable = args[1];
            Iterator<Row> blacklistRows = ctx.getKVS().scan(blacklistTable);
            while (blacklistRows.hasNext()) {
                Row r = blacklistRows.next();
                blacklistPatterns.add(normalizePattern(r.get("pattern")));
            }
        }

        // toggling concurrency level
        ctx.setConcurrencyLevel(10);

        // init url queue from checkpoint or seed URLs
        FlameRDD urlQueue;
        int checkpointCount = ctx.getKVS().count("pt-queue-checkpoint");

        if (checkpointCount > 0) {
            logger.info("reading from checkpoint...");
            urlQueue = ctx.fromTable("pt-queue-checkpoint", s -> s.get("value"));
        } else {
            LinkedList<String> seedURLs = getSeedURLs(args[0]);
            urlQueue = ctx.parallelize(seedURLs);
        }

        // start monitoring thread
        Thread monitorThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    int crawledCount = ctx.getKVS().count("pt-crawl");
                    logger.info("pages crawled so far: " + crawledCount);
                    Thread.sleep(10000); // log every 30 sec
                }
            } catch (InterruptedException e) {
                System.out.println("monitoring thread interrupted.");
            } catch (Exception e) {
                System.err.println("error in monitoring thread: " + e.getMessage());
            }
        });
        monitorThread.start();
        try {

            // main crawling loop
            while (urlQueue.count() > 0) {
                logger.info("URL Queue size: " + urlQueue.count());

                logger.info("deleting old checkpoint...");
                ctx.getKVS().delete("pt-queue-checkpoint");
                logger.info("saving new checkpoint...");
                urlQueue.saveAsTable("pt-queue-checkpoint");


                //start count of queue size to stop adding when it is too large
                AtomicInteger queueSize = new AtomicInteger(urlQueue.count());

                // process  url queue
                urlQueue = urlQueue.flatMap(url -> {
                    long start = System.currentTimeMillis();

                    try {
                        queueSize.decrementAndGet();
                        // hash url for dedup
                        String key = Hasher.hash(url);
                        KVSClient kvs = ctx.getKVS();

                        // skil already visited URLs
                        if (kvs.existsRow("pt-crawl", key)) {
                            //logger.info("Skipping visited page");
                            return Collections.emptySet();
                        }

                        //logger.info("Starting " + url);

                        // handle robots.txt for the URL
                        String[] parsedURL = URLParser.parseURL(url);
                        String host = parsedURL[1];
                        //logger.info("Getting robots for " + url);

                        handleRobots(url, kvs, host);
                        //logger.info("Got robots!");

                        // fetch and process current page
                        //logger.info("Sending " + url + " to crawl. Time elapsed: " + (System.currentTimeMillis() - start));
                        Set<String> extractedUrls = processPage(url, kvs, host);
                        //logger.info("QueueSize: " + queueSize.get());

                        //only add to the queue if we're under the set size
                        if (queueSize.get() < MAX_QUEUE_SIZE) {
                            return extractedUrls;
                        } else {
                                //logger.info("Queue is full. Skipping URLs from " + url);
                                return Collections.emptySet();
                        }


                    } catch (Exception e) {
                        //logger.info("Error processing URL: " + url + " - " + e.getMessage());
                        //logger.info("Something bad happened at " + url + " - " + e.getMessage());
                        return Collections.emptySet();
                    }
                });
            }

        } finally {
            // stop monitoring thread when done
            monitorThread.interrupt();
            monitorThread.join();
        }
    }

    // helper method to handle robots.txt
    private static void handleRobots(String url, KVSClient kvs, String host) throws IOException, URISyntaxException {
        String robotsURL = URLParser.parseURL(url)[0] + "://" + host + ":" + URLParser.parseURL(url)[2] + "/robots.txt";
        String hashedHost = Hasher.hash(host);

        if (!kvs.existsRow("pt-hosts", hashedHost)) {
            //logger.info("pinging host for robots...");
            HttpURLConnection con = (HttpURLConnection) new URI(robotsURL).toURL().openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", "cis5550-crawler");
            con.setConnectTimeout(7000);
            con.setReadTimeout(7000);

            int responseCode;
            try {
                responseCode = con.getResponseCode();

                if (responseCode == 200) {
                    //logger.info("fetching robots...");
                BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
                StringBuilder robotsContent = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    robotsContent.append(line).append("\n");
                }
                reader.close();
                //logger.info("parsing robots...");
                parseRobots(robotsContent.toString(), kvs, hashedHost);
                //logger.info("parse finished");
            } else {
                    //logger.info("parsing robots...");
                parseRobots("", kvs, hashedHost);
                    //logger.info("empty parse finished");
            }
            } catch (IOException e) {
                //logger.info("failed fetching robots.txt for host: " + host + " - " + e.getMessage());
                parseRobots("", kvs, hashedHost);
            }
        }

    }


    // Helper method to process a page
    private static Set<String> processPage(String url, KVSClient kvs, String host) throws IOException, URISyntaxException {
        //long time = System.currentTimeMillis();
        String key = Hasher.hash(url);
        //logger.info("Starting to process page " + url);
        String hashedHost = Hasher.hash(host); // Compute hashedHost here

        /*
        //for head request, if I want to add this
        logger.info("Starting head request " + url);
        HttpURLConnection con = (HttpURLConnection) new URI(url).toURL().openConnection();
        con.setRequestMethod("HEAD");
        con.setRequestProperty("User-Agent", "cis5550-crawler");
        con.setConnectTimeout(7000);
        con.setReadTimeout(7000);
        con.setInstanceFollowRedirects(false);

        int responseCode = con.getResponseCode();
        kvs.put("pt-crawl", key, "url", url);*/

        //logger.info("Checking cached robots file...");
        if (kvs.existsRow("pt-hosts", hashedHost)) {
            Row r = kvs.getRow("pt-hosts", hashedHost);
            if (r.columns().contains("lastAccessed")) {
                if (System.currentTimeMillis() - Long.parseLong(new String(kvs.get("pt-hosts", hashedHost, "lastAccessed"))) <= 1000) {
                    logger.info("Too soon to contact host");
                    return Collections.singleton(url);
                }
            }
        }

        //logger.info("Starting GET");
        //long GETtime = System.currentTimeMillis();
        HttpURLConnection con2 = (HttpURLConnection) new URI(url).toURL().openConnection();
        con2.setRequestMethod("GET");
        con2.setRequestProperty("User-Agent", "cis5550-crawler");
        con2.setConnectTimeout(7000);
        con2.setReadTimeout(7000);
        int responseCode = con2.getResponseCode();
        kvs.put("pt-crawl", key, "url", url);
        kvs.put("pt-hosts", hashedHost, "lastAccessed", String.valueOf(System.currentTimeMillis()));

        if (responseCode > 400) {
            return Collections.emptySet();
        }

        if (responseCode != 200) {

            if (responseCode >= 300 && responseCode < 400) {
                String redirect = con2.getHeaderField("Location");
                if (redirect != null) {
                    //logger.info("Redirected page " + url + " to " + redirect);
                    return Collections.singleton(redirect);
                } else {
                    //logger.info("Throwing out redirect page with no redirect info");
                    return Collections.emptySet();
                }
            }
            //logger.info("Failed to fetch page: " + url + " - Response code: " + responseCode);
            return Collections.emptySet();
        }

        String contentType = con2.getContentType();

        if (contentType == null || !contentType.contains("text/html")) {
            return Collections.emptySet(); // Skip non-HTML pages
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(con2.getInputStream()));
        StringBuilder pageContent = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            pageContent.append(line);
        }
        reader.close();
        //con.disconnect();
        con2.disconnect();
        //logger.info("GET request finished at " + url + ". It took " + (System.currentTimeMillis() - GETtime));

        String pageText = pageContent.toString();
        pageText = pageText.replaceAll("[^\\x00-\\x7F]", "");

        //logger.info("Checking language at " + url);
        if (!isEnglish(pageText)) {
            return Collections.emptySet(); // Skip non-English pages
        }

        // Store page metadata in KVS
        //logger.info("Cleaning page at " + url);
        String cleanPageText = stripPageContent(pageText);
        //logger.info("Page cleaned! : " + url);
        //String hashedContent = Hasher.hash(pageText);

        kvs.put("pt-crawl", key, "url", url);
        kvs.put("pt-crawl", key, "page", cleanPageText);
        String title = getPageTitle(pageText);
        if (title != null) {
            kvs.put("pt-crawl", key, "title", title);
            kvs.put("pt-crawl", key, "titleWords", getTitleWords(title));
        }

        // Extract and return URLs for further crawling
        //logger.info("Time to crawl " + url + " : " + (System.currentTimeMillis() - time));
        return extractURLS(pageText, url, kvs, getAllowDisallowFromTable(kvs, hashedHost));
    }

}