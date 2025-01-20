package cis5550.tools;

import java.util.Arrays;
import java.util.HashSet;

public class Stopwords {
	/*--------------------------------- STOP WORDS ----------------------------------*/
	public static final String[] WORD_ARRAY = new String[] { 
			"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", 
			"S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", 
			"k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "also", "am", 
			"an", "and", "any", "are", "arent", "as", "at", "be", "because", "been", "both", "but", "by", 
			"can", "did", "do", "does", "doing", "dont", "either", "for", "from", "had", "has", "have", 
			"having", "he", "her", "hers", "here", "him", "himself", "his", "how", "hows", "if", "in", 
			"into", "is", "isnt", "it", "itll", "its", "just", "lets", "me", "my", "myself", "no", "nor", 
			"not", "now", "of", "or", "our", "say", "says", "she", "should", "so", "some", "such", "than", 
			"that", "the", "their", "theirs", "them", "themselves", "then", "there", "these", "they", "this",
			"those", "to", "too", "us", "very", "was", "wasnt", "we", "were", "what", "whats", "when", "where",
			"which", "while", "who", "whom", "whose", "whove", "why", "with", "you", "your", "youre", "yours", 
			"yourself", "yourselves"
	};
	public static final HashSet<String> STOP_WORDS = new HashSet<>(Arrays.asList(WORD_ARRAY));
	/*----------------------------------------------------------------------------------*/
}
