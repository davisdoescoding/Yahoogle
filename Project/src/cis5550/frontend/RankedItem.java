package cis5550.frontend;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class RankedItem {

	String urlName;
	private double pagerank;
	private double tfidf;
	private String wordPositions;
	private String title;
	private String isWordInTitle;
	private String isWordInAnchor;
	private String queryTerm;
	
	public RankedItem(
			String queryTerm,
			String url, 
			double pagerank, 
			double tfidf, 
			String wordPositions, 
			String title, 			// the title or "NULL" if no title
			String isWordInTitle, 	// "YES" or "NO"
			String isWordInAnchor	// "YES" or "NO"
			) {
		
		this.queryTerm = queryTerm;
		this.urlName = url;
		this.pagerank = pagerank;
		this.tfidf = tfidf;
		this.wordPositions = wordPositions;
		this.title = title;
		this.isWordInTitle = isWordInTitle;
		this.isWordInAnchor = isWordInAnchor;
		
	}
	
	public String getQueryTerm() {
		return this.queryTerm;
	}
	
	public String getUrlName() {
		return this.urlName;
	}
	
	public String[] getWordPositions() {
		return this.wordPositions.split("-");
	}
	
	public String getTitle() {
		return this.title;
	}
	
	public String getTitleWords() {
		return this.isWordInTitle;
	}
	
	public String getAnchorWords() {
		return this.isWordInAnchor;
	}
	
	public double getPageRank() {
		return this.pagerank;
	}
	
	public void setPageRank(double rank) {
		this.pagerank = rank;
	}
	
	public double getTFIDF() {
		return this.tfidf;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == this) {
			return true;
		}
		
		if(!(o instanceof RankedItem)) {
			return false;
		}
		
		RankedItem r = (RankedItem)o;
		
		return Double.compare(pagerank, r.pagerank) == 0 && urlName.equals(r.urlName);
	}
}
