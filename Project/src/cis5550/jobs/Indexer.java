package cis5550.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.*;

public class Indexer {
	private static final Logger logger = Logger.getLogger(Indexer.class);
	
	public static void run(FlameContext fc, String[] args) throws Exception {
		//-----------load data from pt-crawl table into FlamePairRDD-----------------
		
		//use FlameContext.fromTable
		//row is a Row object
		FlameRDD rdd = fc.fromTable("pt-crawl", row -> {
			Row r = row;
			String url = r.get("url");
			if(url == null) {
				return null;
			}
			byte[] page = r.getBytes("page");//can this be null? - i think so since still create row for urls returning error codes or redirects
			String title = r.get("title");
			String titleWords = r.get("titleWords");

			if(page == null) {
				return null;
			}
			
			String pageString = new String(page);

			if (pageString.isBlank()) {
				return null;
			}
			
			/* 
			 * remove HTML tags, punctuation, tabs, CR, LF before building URL string to avoid errors 
			 * later on splitting/concatenating on comma/colon delimiters
			 */
			StringBuilder pageSB = new StringBuilder(pageString);
			
			//filter out all HTML tags <*>
			//use regex matcher with .start and .end to get start index and end index+1 
			Pattern patt = Pattern.compile("[<].*?[>]");
			Matcher m = patt.matcher(pageString);
			
			while(m.find()) {
				//testing with replacing html tag with " " because some are directly between words ex: hello<h1>world</h1>how
				pageSB.replace(m.start(), m.end(), " ");
				m = patt.matcher(pageSB);
			}
						
			//remove all punctuation
			patt = Pattern.compile("[.,:;!?'\"()-]");
			m = patt.matcher(pageSB);
			
			while(m.find()) {
				pageSB.replace(m.start(), m.end(), " ");
				m = patt.matcher(pageSB);
			}
			
			//remove tabs, CR, LF by replacing with " "  to ensure words don't merge together
			patt = Pattern.compile("[\r\n\t]");
			m = patt.matcher(pageSB);
			
			while(m.find()){
				pageSB.replace(m.start(), m.end(), " ");
				m = patt.matcher(pageSB);
			}
			
			//convert everything to lowercase
			pageString = pageSB.toString().toLowerCase();
			pageString.replaceAll("[^\\x00-\\x7F]", " ");
			
			
			StringBuilder sb = new StringBuilder();

			/* url */
			sb.append(url);
			sb.append(",");
			/* page */
			sb.append(pageString);
	        sb.append(",");
	        /* title */
	        sb.append(title != null ? title : "NULL");
	        sb.append(",");
	        /* title words */
	        if (titleWords == null) {
	        	titleWords = "NULL";
	        }
	        else {
	        	titleWords = titleWords.replaceAll("[!()^;\"#$&%\'*+./@?_]", "");
	        	titleWords = titleWords.replace(",", "-");
	        }
	        sb.append(titleWords);
            sb.append(",");
            /* anchor words */
            String anchorWords = "NULL";
            Row anchorRow;
			try {
				anchorRow = fc.getKVS().getRow("pt-anchors", r.key());
				if(anchorRow != null && !(anchorRow.get("anchorWords") == null)) {
					anchorWords = anchorRow.get("anchorWords");
					anchorWords = anchorWords.replaceAll("[!()^;\"#$&%\'*+./@?_]", "");
					anchorWords = anchorWords.replace(",", "-");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
            sb.append(anchorWords);
			
            return sb.toString();  // url, pageString, title, titleWords, anchorWords
		});
		
		
		//use mapToPair on it to convert to a PairRDD
		//s is row with url as key and page as value
		//create (u,p) FlamePair
		FlamePairRDD pairRdd = rdd.mapToPair(s -> {	
			//Split it so we have url, page, title, titleWords, anchorWords -> split into 5 pieces
			String[] parts = s.split(",", 5);
			return new FlamePair(parts[0], parts[1] + ":" + parts[2] + ":" + parts[3] + ":" + parts[4]);
		});
				
		
		//------------create inverted index-------------------------------
		
		//convert each (u,p) pair to lots of (w,u) pairs - w is word that occurs in p		
		FlamePairRDD wordPairs = pairRdd.flatMapToPair(p -> {
			
			FlamePair pair = p;

			String url = pair._1();
			String[] parts = ((String)pair._2()).split(":");
			   String page;
	            String title;
	            String titleWords;
	            String anchorWords;
            try {
            page = parts[0];
           title = parts[1];
           titleWords = parts[2];
           anchorWords = parts[3];
            }catch(Exception e) {
            	List<FlamePair> pairs1 = new ArrayList<>();
            	return pairs1;
            }
			
			//now have a string with words separated by " "
			String[] wordArray = page.split(" ");
			
			/* Remove stop words */
			//wordList has all words (except STOP WORDS) from page data with duplicates
			List<String> wordListNoStopWords = new ArrayList<String>();
			Pattern patt = Pattern.compile("[^\\w~!@#$%&*)(\\-=;:'\\\"<>?,./`|\\\\]");
			for (String word : wordArray) {
				if (Stopwords.STOP_WORDS.contains(word)) {
					continue;
				}
				if (word.isEmpty()) {
					continue;
				}
				if(word.startsWith("0")) {
					continue;
				}
				if((word.length() > 300) || ((int)word.charAt(0) == 0)) {
					logger.debug(word + " \n " + word.charAt(0) + " \n " + (int)word.charAt(0));
					continue;
				}
				
				Matcher m = patt.matcher(word);
				if(m.find()) {
					continue;
				}
				
				logger.debug("stop list loop word: " + word + " length: " + word.length());
				wordListNoStopWords.add(word);
			}
			
			/* Word Count
			 * 
			 * Construct a hyphen-delimited string of word positions, ex) "1-5-34-165"
			 * 
			 * Need to get word positions before calculate TF scores because calculating
			 * TF scores removes duplicate words.
			 */
			HashMap<String, List<String>> wordPositions = new HashMap<>();
			for (int i = 0; i < wordListNoStopWords.size(); i++) {
                String word = wordListNoStopWords.get(i);
                //Start new list if this is the first time we're seeing this word
                if (!wordPositions.containsKey(word)) {
                    wordPositions.put(word, new ArrayList<>());
                }
                //Add the position to the list
                wordPositions.get(word).add(String.valueOf(i));                            
            }
            //convert to the string format "word: position1 position2" etc
            HashMap<String, String> wordToPositionString = new HashMap<>();
            for (Map.Entry<String, List<String>> wordPosition : wordPositions.entrySet()) {
                String word = wordPosition.getKey();
                List<String> positionList = wordPosition.getValue();
                StringBuilder positionString = new StringBuilder();
                for (int i = 0; i < positionList.size(); i++) {
                    positionString.append(positionList.get(i));
                    if (i != positionList.size()-1) {
                        positionString.append("-");
                    }
                }
                wordToPositionString.put(word, positionString.toString());
            } /* End Word Count */
			
			//calculate TF score for each term in the url
			//this will remove duplicates as well
			Map<String, Double> tfScores = processTFData(wordListNoStopWords);
			
			// create list of pairs (w,u TFscore wordPosition title titleWords anchorWords) with the url
		    List<FlamePair> pairs = new ArrayList<>();
		    for (Map.Entry<String, Double> tfEntry : tfScores.entrySet()) {
		        String term = tfEntry.getKey();
		        double tfScore = tfEntry.getValue();
				logger.debug("indexer- " + url + " :word: " + term + ":tfscore:" + tfScore);
				if(term != null && term.length() > 0) {
					pairs.add(
							new FlamePair(term, 
									url + " " + tfScore + " " + wordToPositionString.get(term) + " " + title + " " + titleWords + " " + anchorWords)
					);
				}
		    }
		    return pairs;
		});
		
		//fold (w, ui) pairs using foldByKey
		FlamePairRDD invertedIndex = wordPairs.foldByKey("0", (String a, String b) -> {
			//accumulator is going to be concatenation of every value with "," so we get the (w, u) list we need? where u is u1,u2,u3,u4...
//			if(a.length() > 0) {
			if(!a.equals("0")) {
				logger.debug("indexer- foldByKey a: " + a + " b: " + b);
				if(a.contains(b)) {//if url b is already in list a, don't add and just return the current list
					return a;
				}else {
					return a + "," + b;
				}
			}else {
				return b;
			}
		});
		
		
		Map<String, Double> docFrequency = new HashMap<>();
		
		// get invertedIndex data to get df values
		List<FlamePair> output = invertedIndex.collect();
		for (FlamePair pair : output) {
		    String term = pair._1();
		    String urls = pair._2();
		    
		    // get total number of urls that contain the term
		    double df = urls.split(",").length;
		    // store df for term
		    docFrequency.put(term, df);
		}
		
		// total number of urls - "docs"
        double totalDocuments = (double) pairRdd.collect().size();

        FlamePairRDD tfidfIndex = wordPairs.flatMapToPair(p -> {
            String term = p._1();
            
            //convert each term, url TFscore wordPosition title titleWords anchorWords pair to w, url TfIdf wordPosition title titleWords anchorWords pairs
            String urlAndTf = p._2();
            logger.debug("urlAndTf: " + urlAndTf);
            
            /* Changed the delimiter from COLON to SPACE in wordPairs lambda */
            String[] split = urlAndTf.split(" ");
            String url = split[0];
            String tfString = split[1];
            String wordPosition = split[2];
            String title = split[3];
            String titleWords = split[4];
            String anchorWords = split[5];
            
            logger.debug(
            	"url: " + url + 
            	" tfString: " + tfString + 
            	" wordPosition: " + wordPosition + 
            	" title: " + title + 
            	" titleWords: " + titleWords + 
            	" anchorWords: " + anchorWords
            );
            
            double tf = Double.parseDouble(tfString);

			// calculate idf for term
            double df = docFrequency.get(term);
            double idf = Math.log((double) totalDocuments / df);

			// calculate tfidf for term
            double tfidf = tf * idf;
            
            // process to find if there the url has the term in the title or anchor tags
            titleWords = processText(term, titleWords);
            anchorWords = processText(term, anchorWords);

            // -> (term, url tfidfScore wordPosition title (YES or NO if url has the term in the title) (YES or NO if url has the term in the anchor tag))
            return Collections.singletonList(new FlamePair(term, url + " " + tfidf + " " + wordPosition + " " + title + " " + titleWords + " " + anchorWords));
        });
        
		//fold (term, url tfidfScore wordPosition title (YES or NO if url has the term in the title) (YES or NO if url has the term in the anchor tag)) pairs using foldByKey for final invertedIndex
		FlamePairRDD finalInvertedIndex = tfidfIndex.foldByKey("0", (String a, String b) -> {
			//accumulator is going to be concatenation of every value with "," so we get the (w, u:tfidf) list we need
//			if(a.length() > 0) {
			if(!a.equals("0")) {
				logger.debug("final indexer- foldByKey a: " + a + " b: " + b);
				if(a.contains(b)) {//if url b is already in list a, don't add and just return the current list
					return a;
				}else {
					return a + "," + b;
				}
			}else {
				return b;
			}
		});
		
		finalInvertedIndex.saveAsTable("pt-index");
		
	}
	
	/* titleWords and anchorWords split on HYPHEN (commas replaced with hyphens b/c string splitting errors) */
	private static String processText(String term, String words) {
		// words contain comma-separated strings of words from pt-crawl table with titleWords and anchorWords
		if (!words.equals("NULL")) {
			String[] split = words.split("-");
			for (String word : split) {
				if (word.equalsIgnoreCase(term)) return "YES";
			}
		}
		return "NO";
	}

	public static Map<String, Double> processTFData(List<String> wordList) {
		Map<String, Integer> tfData = new HashMap<>();

		double totalTerms = wordList.size();
		
		// for each word update the number of times the term appears in the page
		for (String word : wordList) {
			logger.debug("tfData word: " + word);
			// increment count within the url
	        tfData.put(word, tfData.getOrDefault(word, 0) + 1);
		}
		
		// map to store tf scores fpr this url
	    Map<String, Double> score = new HashMap<>();

		//for each word calculate the TF of each term
		for (Map.Entry<String, Integer> entry : tfData.entrySet()) {			
		    String term = entry.getKey();
		    double wordCount = entry.getValue();
		    
//		    System.out.println("Term: " + term + ", Count: " + wordCount);
		    
		    //calculate TF score
		    double tfScore = wordCount / totalTerms;
		    
		    //store score for term
		    score.put(term, tfScore);		    
		}
		
		return score;	
	}
}
