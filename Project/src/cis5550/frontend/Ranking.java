package cis5550.frontend;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import cis5550.tools.URLParser;

public class Ranking {

	/* 
	 * Homepage will access FINAL_RANKS and URL_TO_RANKEDITEM to display TFIDF and PageRank for testing
	 */
//	public static Map<String,Double> FINAL_RANKS = new TreeMap<String,Double>();
//	public static Map<String,RankedItem> URL_TO_RANKEDITEM = new TreeMap<String,RankedItem>();
	

	// TODO: Simple ranking for now: just add all scores
	public static List<String> calculateRanking(List<RankedItem> rankedItemList) {

		Map<String,Double> urlToRank = new TreeMap<String,Double>();

		for (RankedItem item : rankedItemList) {

			String queryTerm = item.getQueryTerm();
			String url = item.getUrlName();
			double pagerank = item.getPageRank();
			double tfidf = item.getTFIDF();
			String[] wordPositions = item.getWordPositions();
			String title = item.getTitle();
			String isWordInTitle = item.getTitleWords();
			String isWordInAnchor = item.getAnchorWords();
			
			
//			URL_TO_RANKEDITEM.put(url, item);
			

			//add weights to each value before adding together
			//anchorWords and titleWord have weights applied, then added together. Then apply final weight of 30% due to the way it is scored
			double finalRank = 
					(pagerank * 0.2)
					+ (tfidf * 0.30)
					+ (((Ranking.getAnchorWordScore(isWordInAnchor)  * 0.333)
					+ (Ranking.getTitleWordScore(isWordInTitle) * 0.667)) * 0.30 )
					+ (Ranking.getWordPositionScore(wordPositions) * 0.10)
					+ (Ranking.getDomainNameScore(url, queryTerm) * 0.10);

			if (urlToRank.containsKey(url)) {
				double rank = urlToRank.get(url);
				urlToRank.put(url, rank + finalRank);
			}
			else {
				urlToRank.put(url, finalRank);
			}
		}

		// Set the static Map to the current query ranking; use this map in Homepage.java to display ranking.
//		Ranking.FINAL_RANKS = urlToRank;

		// List of URLs ranked in descending order
		List<String> urlsRankedDescending = 
				urlToRank.entrySet()
				.stream()
				.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());

		
//		/* DEBUGGING: print all URL-to-Rank entries */
//		for (Entry <String, Double> entry : urlToRank.entrySet()) {
//			String url = entry.getKey();
//			double rank = entry.getValue();
//			System.out.println("* "+ url);
//			System.out.println("------> " + rank);
//		}
//		
//		/* DEBUGGING: print just the URLs in descending order by rank, does not include the ranking */
//		System.out.println("\nFinal Ranks:\n");
//		for (int i = 0; i < urlsRankedDescending.size(); i++) {
//			System.out.println(i+1 + ") " + urlsRankedDescending.get(i));
//		}
		
		
		return urlsRankedDescending;
	}

	// TODO: These are just basic ideas to get started. Please feel free to change things 
	// if/when anyone has other ideas or improvements :)
	
	static double getDomainNameScore(String url, String queryTerm) {

		double score = 0.0; 
		
		String[] pieces = URLParser.parseURL(url);
		
		// domain base url
		if (pieces[1].equalsIgnoreCase(queryTerm)) {
			score += 1.0;
		}
		
		if (pieces[1].contains(queryTerm)) {
			score += 1.0;
		}
		// domain path
		if (pieces[3].contains(queryTerm)) {
			score += 1.0;		
		}
		
		// normalize the score by dividing by max score possible to scale it to match tfidf and pagerank scale
		return score / 3.0;

		
	}

	static double getWordPositionScore(String[] wordPositions) {
		/*
		 * The idea is to give heavier weight to URLs that have:
		 * 1) more of this term on its page
		 * 2) have this word occurring at lower positions (should indicate this
		 * word is more relevant if it's near the beginning / top of the page).
		 */

		double score = 0.0;

		// total occurrences of the word on the page
	    double totalWords = wordPositions.length;
	    
	    if (totalWords == 0.0) {
	    	// to prevent division by zero
	        return 0.0;
	    }
		
	    for (String position : wordPositions) {
	        int pos = Integer.parseInt(position);
	        if (pos <= 500.0) {
	        	// give higher score for words within the first 500 words
	            score += 2.0; 
	        } else {
	        	// still give some score for words outside the first 500 words
	            score += 1.0; 
	        }
	    }
	    
	    // Normalize the score by the total number of occurrences
	    return score / totalWords; 
	}

	// titleWords = "YES" or "NO" or "NULL"
	static int getTitleWordScore(String titleWords) {
		/*
		 * If the query term is in the title simply increment this score by 1.
		 */

		int score = 0;
		
		if (titleWords.equals("YES")) {
			score = 1;
		}
		
		return score;
	}

	// anchorWords = "YES" or "NO" or "NULL"
	static int getAnchorWordScore(String anchorWords) {
		/*
		 * If the query term is in the anchor simply increment this score by 1.
		 */

		int score = 0;
		
		if (anchorWords.equals("YES")) {
			score = 1;
		}
		
		return score;
	}
}
