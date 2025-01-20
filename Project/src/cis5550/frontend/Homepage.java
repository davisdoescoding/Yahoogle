package cis5550.frontend;

import static cis5550.webserver.Server.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.*;

import cis5550.frontend.*;

public class Homepage extends Thread {

	private static final Logger logger = Logger.getLogger(Homepage.class);
	
	/* Use this map to get page titles to display search results */
	private static Map<String,String> PAGE_TITLES = new ConcurrentHashMap<String,String>();
	
	/* Cached search results from previous searches
	 * 
	 * Cache of query results in-memory (the List of ranked URLs). If a previous query is 
	 * queried again (has to be same words in the same order) we don't need to access the
	 * persistent tables and calculate ranking again, we can just return the list of URLs
	 * from in-memory, much faster.
	 */
	private static Map<String,List<String>> RANKED_RESULTS_CACHE = new HashMap<String,List<String>>();
	
	/* Cached RankedItems from the pt-index table
	 * 
	 * Every individual term being queried will have row in pt-index (if exists) that contains
	 * the URLs and metadata for that specific term. The number of RankedItems per term is based
	 * on the number of the URLs. INDEX_CACHE will map a pt-index term to all of its RankedItems.
	 * Every term being queried will cache its RankedItems in INDEX_CACHE so if a term is being
	 * queried again we can get all of its RankedItems without have to access the pt-index -- 
	 * this much faster. This is not the same as RANKED_RESULTS_CACHE, which returns the actual
	 * URLs in ranked order.
	 */
	private static Map<String,List<RankedItem>> INDEX_CACHE = new ConcurrentHashMap<String,List<RankedItem>>();
	
	
	/* Zip Codes */
	private static Map<String,String> ZIP_CODES = new HashMap<String, String>();
	

	public static KVSClient kvs;

	private final static String bootstrapStylesheet = "<link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\" integrity=\"sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH\" crossorigin=\"anonymous\">";
	private final static String bootstrapScript = "<script src=\"https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js\" integrity=\"sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz\" crossorigin=\"anonymous\"></script>";


	public static String basePage() {

		return "<form action=\"/results\"><div class=\"row justify-content-center align-items-center\">"
				+ "<input class=\"col-md-5\" type=\"text\" id=\"searchTerms\" name=\"searchTerms\">"
				+ "<input class=\"col-sm-1\" type=\"submit\" value=\"Submit\"></div></form>"; 

	}

	public static String resultsPage(String searchTerms) {
		return "<form action=\"/results\"><div class=\"row ms=3 ps-3\">"
				+ "<input class=\"col-md-5\" type=\"text\" id=\"searchTerms\" name=\"searchTerms\" value=\"" + searchTerms +  "\">"
				+ "<input class=\"col-sm-1\" type=\"submit\" value=\"Submit\"></div></form>";
	}
	
	/*
	 * Search contains exclusively stop words, return hardcoded results
	 */
	private static List<String> stopwordSearch(List<String> allSearchTermsList) {
		// https://www.dictionary.com:443/browse/<WORD>
		// https://www.merriam-webster.com:443/dictionary/<WORD>
		// https://en.wikipedia.org:443/wiki/<WORD>
		// https://www.thesaurus.com:443/browse/<WORD>
		
		List<String> stopWordURLs = new ArrayList<String>();
		
		for (String stopWord : allSearchTermsList) {
			String dictionary = "https://www.dictionary.com:443/browse/" + stopWord;
			String webster = "https://www.merriam-webster.com:443/dictionary/" + stopWord;
			String wikipedia = "https://en.wikipedia.org:443/wiki/" + stopWord;
			String thesaurus = "https://www.thesaurus.com:443/browse/" + stopWord;
			
			stopWordURLs.add(dictionary);
			stopWordURLs.add(webster);
			stopWordURLs.add(wikipedia);
			stopWordURLs.add(thesaurus);
		}
		
		return stopWordURLs;
	}
	
	/*
	 * Get search results from index
	 */
	private static List<RankedItem> termSearch(List<String> termsList) {
		
		List<RankedItem> rankedItemList = new ArrayList<RankedItem>();
	
		/* Get a row from pt-index with a thread, instantiate a RankedItem from the row, add it to rankedItemArray */
		Thread threads[] = new Thread[termsList.size()];
		for (int i = 0; i < termsList.size(); i++) {

			final int j = i;

			threads[i] = new Thread("Thread #" + (i + 1)) {
				public void run() {
					try {
						String queryTerm = termsList.get(j);
						
						// we already searched for this individual term so we can get its RankedItems 
						// from a cache instead of accessing the persistent tables -- much faster
						if (INDEX_CACHE.containsKey(queryTerm)) {
							List<RankedItem> rankedItems = INDEX_CACHE.get(queryTerm);
							for (RankedItem ri : rankedItems) {
								rankedItemList.add(ri);
							}
						}
						else {
							//look up words in index - pt-index
							Row r = kvs.getRow("pt-index", queryTerm);
							if(r != null) {
								
								// new ranked items that will be added to the INDEX_CACHE
								List<RankedItem> rankedItemsForIndexCache = new ArrayList<RankedItem>();
								
								String urls = r.get("acc");
								String[] urlWithRankingData = urls.split(",");

								for(String str : urlWithRankingData) {

									// str == "[url] [tfidfScore] [wordPosition] [title] [titleWords]"
									String[] data = str.split(" ");

									String url = data[0];
									Double tfidfScore = Double.parseDouble(data[1]);
									String wordPositions = data[2];
									String title = data[3]; 		  // the title or "NULL" if no title
									String isWordInTitle = data[4];	  // "YES" or "NO"
									String isWordInAnchor = data[5];  // "YES" or "NO"

									// reform title: replace hyphens with spaces 
									if (!title.equals("NULL")) {
										title = title.replace("-", " ");	
									}

									if (!PAGE_TITLES.containsKey(url)) {
										PAGE_TITLES.put(url, title);
									}

									// pt-pagerank and pt-anchors row key is hash of URL -- don't use Flame
									String key = Hasher.hash(url);

									// get pagerank
									Row pr = kvs.getRow("pt-pageranks", key);
									
									// in case no page rank for the url set pagerank to 0.0
									Double pageRank = 0.0;
									if (pr != null) {
										pageRank = Double.parseDouble(pr.get("rank"));
									}
									
									RankedItem ri = new RankedItem(queryTerm, url, pageRank, tfidfScore, wordPositions, title, isWordInTitle, isWordInAnchor);

									rankedItemList.add(ri);
									
									rankedItemsForIndexCache.add(ri);
								}
								
								// put the new items to the INDEX_CACHE
								INDEX_CACHE.put(queryTerm, rankedItemsForIndexCache);
							}
							// term not in index; also cache it in INDEX_CACHE in case the term is searched again
							else {
								INDEX_CACHE.put(queryTerm, new ArrayList<RankedItem>());
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			threads[i].start();
		}

		// Wait for threads to finish
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
		
		return rankedItemList;
	}

	
	/*---------------------------------------------------------------- main ----------------------------------------------------------------*/
	
	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Syntax: Homepage <port> <kvsCoordinator>");
			System.exit(1);
		}

		int myPort = Integer.valueOf(args[0]);
		logger.info("Homepage starting on port "+myPort);

		kvs = new KVSClient(args[1]);
		logger.debug("homepage kvs: " + kvs.getCoordinator());

//		port(myPort);
		securePort(443);
		port(80);
		
		/* read zip code file, populate ZIP_CODES map */
		URL path = Homepage.class.getResource("zipCodes.txt");
		try {
			File f = new File(path.getFile());
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] arr = line.split("\t");
	            String zip = arr[1];
	            String city = arr[2];
	            String state = arr[3];
	            if (zip.isBlank() || city.isBlank() || state.isBlank()) {
	            	continue;
	            }
	            ZIP_CODES.put(zip, city+","+state);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		/*-----------------pages-------------------*/

		//--- Landing page ----
		get("/", (request,response) -> {
			response.type("text/html");

			return "<html><head><title>Yahoogle Homepage</title></head><body><div class=\"container text-center\"><div class=\"row justify-content-center align-items-center\"><h2 class=\"fs-1 my-5\">Yahoogle Search</h2>\n" 
			+ basePage() + "</div></div>"   
			+ bootstrapStylesheet
			+ bootstrapScript
			+ "</body></html>";
		});

		//--- Results page ---
		get("/results", (request, response) -> {
			response.type("text/html");

			int startIndex = 0;
			if(request.queryParams("start") != null) {
				startIndex = Integer.parseInt(request.queryParams("start"));
			}

			//get query params
			String terms = request.queryParams("searchTerms");
			terms = terms.strip();
			
			// zip code vars
			boolean hasZipCode = false;
			String googleMapsLocation = null;

			//split into individual words
			String[] searchTerms = terms.split(" ");

			List<String> allSearchTermsList = new ArrayList<String>();
			List<String> termsList = new ArrayList<String>();

			// 1. remove punctuation from search query
			// 2. add all terms to allSearchTermsList
			// 3. skip stop words
			for (String term : searchTerms) {
				// multiple spaces could produce empty string after splitting
				if (term.isEmpty()) {
					continue;
				}
				
				term = term.replaceAll("[!()^;\"#$,&%\'*+./@?\\]\\[_]", "");
				// think hyphenated words should be counted as individual terms
				term = term.replace("-", " ").toLowerCase();

				String[] hyphenSplit = term.split(" ");
				
				for (String s : hyphenSplit) {
					if (s.isEmpty()) {
						continue;
					}
					allSearchTermsList.add(s);
					if (Stopwords.STOP_WORDS.contains(s)) {
						continue;
					}
					termsList.add(s);
					
					String cityState = ZIP_CODES.getOrDefault(s, null);
					if (cityState != null) {
						hasZipCode = true;
						googleMapsLocation = "https://www.google.com/maps/place/" + cityState;
					}
				}
			}
			
			// exact search terms joined into single string, ex) "new york pizza" -> "new,york,pizza"
			String joinedTerms = String.join(",", termsList);
			
			// The list of URLs ranked in descending order; initially empty in case no search results
			List<String> urlsRankedDescending = new ArrayList<String>();  
			
			// all search terms are exclusively stop words --> stopwordSearch
			if (Stopwords.STOP_WORDS.containsAll(allSearchTermsList)) {
				urlsRankedDescending = stopwordSearch(allSearchTermsList);
			}
			// we already searched for this, get results from cache
			else if (RANKED_RESULTS_CACHE.containsKey(joinedTerms)) {
				urlsRankedDescending = RANKED_RESULTS_CACHE.get(joinedTerms);
			}
			// otherwise do search for terms in pt-index --> termSearch
			else {
				List<RankedItem> rankedItemList = termSearch(termsList);
				urlsRankedDescending = Ranking.calculateRanking(rankedItemList);
				// include google maps zip code as first result if query has valid zip code
				if (hasZipCode) {
					urlsRankedDescending.add(0, googleMapsLocation);
				}
				// Cache search results in-memory for faster lookup next time exact term(s) queried
				RANKED_RESULTS_CACHE.put(joinedTerms, urlsRankedDescending);
			}

			/*--------- At this point urlsRankedDescending contains all URLs in descending order by rank ---------*/
			
			
			// TOP 200 RESULTS
			if (urlsRankedDescending.size() >= 200) {
				List<String> top200Links = new ArrayList<String>(); 
				for (int i = 0; i < 200; i++) {
					top200Links.add(urlsRankedDescending.get(i));
				}
				urlsRankedDescending = top200Links;
			}
			
			
			//no search results, skip computations
			if(urlsRankedDescending.size() == 0) {

				return "<html><head><title>Yahoogle Search Results</title><div class=\"container\"></head><body><h2 class=\"my-3\"><a href=\"/\" style=\"text-decoration:none\">Yahoogle Search Results</a></h2>\n"
						+ "<p>Searched for: " + terms + "</p>" + resultsPage(terms)
						+ "<p>No results match your search</p>"
						+ "</div>"
						+ bootstrapStylesheet
						+ bootstrapScript
						+"</body></html>";
			}

			/*----------- ASSEMBLE URL TABLE -------------*/

			//assemble results page
			StringBuilder sb = new StringBuilder();

			/* Map of URL:ranking sorted in descending order by its ranking, (used for testing to display rank in results). */
//			Map<String,Double> FINAL_RANKS = Ranking.FINAL_RANKS;

			for(int i=startIndex; i < urlsRankedDescending.size() && i < (startIndex+10); i++) {
				String url = urlsRankedDescending.get(i);
				String title = PAGE_TITLES.get(url);
				
//				// TODO
//				/* 
//				 * These just for testing, we need to comment these out before submission. 
//				 *
//				 * FYI: The finalRank is incorrect when querying a cached term, but the actual URL 
//				 * ranks displayed on Homepage are correct.
//				 */
//				/**********************************************************************************************/
//				String finalRank;
//				String tfidf;
//				String pagerank;
//
//				if (Stopwords.STOP_WORDS.containsAll(allSearchTermsList)) {
//					finalRank = "stop word";
//					tfidf = "stop word";
//					pagerank = "stop word";
//				}
//				else {
//					finalRank = Double.toString(FINAL_RANKS.getOrDefault(url, 0.0));
//					tfidf = Double.toString(Ranking.URL_TO_RANKEDITEM.get(url).getTFIDF());
//					pagerank = Double.toString(Ranking.URL_TO_RANKEDITEM.get(url).getPageRank());
//				}
				/**********************************************************************************************/

				// if title == "NULL" or is null then the page had no title, display URL domain name instead
				if (title == null || title.equals("NULL")) {
					if (hasZipCode) {
						String cityAndState = URLParser.parseURL(url)[3].split("/")[3];
						String city = cityAndState.split(",")[0];
						String state = cityAndState.split(",")[1];
						title = city + ", " + state;
					}
					else {
						title = URLParser.parseURL(url)[1];
					}					
				}
				
				// remove port from URL displayed over title
				String urlNoPort = url.replace(":443", "");

//				sb.append(
//						"<tr><td>" +  "<a style=\"text-decoration:none\" href=\"" + url + "\"><div><p class=\"text-dark mb-0 fs-8\">"+ urlNoPort + "</p><h3 class=\"fs-5\">" + title + "</h3></div></a></td>" 	
//						+ "<td>" +  finalRank + "</td>" // final rank
//						+ "<td>" +  tfidf + "</td>" 	// TF-IDF
//						+ "<td>" +  pagerank + "</td>" 	// PageRank
//						+ "</tr>");
				
				
				// REMOVED RANKS FROM HOMEPAGE
				sb.append(
						"<tr><td>" +  "<a style=\"text-decoration:none\" href=\"" + url + "\"><div><p class=\"text-dark mb-0 fs-8\">"+ urlNoPort + "</p><h3 class=\"fs-5\">" + title + "</h3></div></a></td>" 	
						+ "</tr>");
			}

			String urlTable = sb.toString();


			/*----------- PAGINATION SECTION ---------*/

			//get # of pages, 10 to a page
			int numPages = (int) Math.ceil((double)urlsRankedDescending.size() / (double)10);

			StringBuilder sbPage = new StringBuilder();

			//whether to disable previous button or not
			if(startIndex == 0) {
				sbPage.append("<li class=\"page-item disabled\"><span class=\"page-link\" >Previous</span></li>");
			}else {
				int prevStart = startIndex - 10;
				sbPage.append("<li class=\"page-item\"><a class=\"page-link\" href=\"/results?searchTerms=" + terms + "&start=" + prevStart + "\">Previous</a></li>");
			}

			//iterate through pages
			int startPageNum = (startIndex/10) +1;
			int currentPage = startPageNum;
			int currentIndex = startIndex;

			if(currentPage >=6) {
				//show 5 pages prior and up to 4 after
				//startpage = current-5
				//end page == current + 4 if not greater than total pages
				for(int pageNum = startPageNum - 5, i = startIndex - 50, currPage =startPageNum; i < urlsRankedDescending.size() && pageNum <= (startPageNum + 4); i += 10, pageNum++) {
					if(i != currentIndex) {
						sbPage.append("<li class=\"page-item\"><a class=\"page-link\" href=\"/results?searchTerms=" + terms 
								+ "&start=" + i + "\">" + pageNum + "</a></li>");
					}else {
						sbPage.append("<li class=\"page-item active\"><span class=\"page-link\">"+ pageNum +"</span></li>");
					}
				}
			}else {
				//show 1 through currentPage
				//show 10 - current page more if exists
				for(int pageNum = 1, i = 0, currPage =startPageNum; i < urlsRankedDescending.size() && pageNum <= 10; i += 10, pageNum++) {
					if(i != currentIndex) {
						sbPage.append("<li class=\"page-item\"><a class=\"page-link\" href=\"/results?searchTerms=" + terms 
								+ "&start=" + i + "\">" + pageNum + "</a></li>");
					}else {
						sbPage.append("<li class=\"page-item active\"><span class=\"page-link\">"+ pageNum +"</span></li>");
					}
				}
			}
			
			
			//whether to disable next button or not
			int nextStart = startIndex +10;
			if(nextStart < urlsRankedDescending.size()) {
				sbPage.append("<li class=\"page-item\"><a class=\"page-link\" href=\"/results?searchTerms=" + terms + "&start=" + nextStart + "\">Next</a></li>");
			}else {
				sbPage.append("<li class=\"page-item disabled\"><span class=\"page-link\">Next</span></li>");
			}



			return "<html><head><title>Yahoogle Search Results</title><div class=\"container\"></head><body><h2 class=\"my-3\"><a href=\"/\" style=\"text-decoration:none\">Yahoogle Search Results</a></h2>\n"
			+ "<p>Searched for: " + terms + "</p>" + resultsPage(terms)
			+ "<table class=\"table\">"
//			+ "<tr><th>Url</th><th>Final Rank</th><th>TF-IDF</th><th>PageRank</th></tr>" 
			+ urlTable + "</table>"
			+ "<div><ul class=\"pagination\">"
			+ sbPage.toString()
			+ "</ul></div>"
			+ "</div>"
			+ bootstrapStylesheet
			+ bootstrapScript
			+"</body></html>";
		});

	}

}
