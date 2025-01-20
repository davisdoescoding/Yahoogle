package cis5550.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

public class PageRank {

	private static final Logger logger = Logger.getLogger(PageRank.class);

	public static ArrayList<String> extractURL(String pageString) {
		ArrayList<String> extractedUrls = new ArrayList<String>();

		//use regex to match on <...>
		Pattern p = Pattern.compile("[<].*?[>]");
		Matcher matcher = p.matcher(pageString);

		ArrayList<String> tags = new ArrayList<String>();

		//get all <...> matches in the page and add them to arraylist
		while(matcher.find()) {
			tags.add(matcher.group());
		}

		//go through all tags, ignore </ tags, split others by " "
		for(String s : tags) {
			if(!s.startsWith("</")) {
				//split on spaces
				String[] split = s.split(" ");
				if(split[0].toLowerCase().startsWith("<a")){
					for(int i=1; i < split.length; i++) {
						if(split[i].toLowerCase().startsWith("href")) {
							String[] hrefSplit = split[i].split("=", 2);
							//trim out leading " and trailing "> and add to ArrayList
							//TODO change this to split on " and keep what's in betwen them
							if(hrefSplit[1].endsWith("\"")) {
								extractedUrls.add(hrefSplit[1].substring(1, hrefSplit[1].length()-1));
							}else {
								extractedUrls.add(hrefSplit[1].substring(1, hrefSplit[1].length()-2));
							}
						}
					}
				}
			}
		}

		return extractedUrls;
	}


	public static ArrayList<String> normalizeAndFilterURL(ArrayList<String> url, String baseUrl){
		ArrayList<String> normalizedArray = new ArrayList<String>();

		String[] baseSplit = URLParser.parseURL(baseUrl);

		for(String s: url) {
			//			System.out.println("url: " + s);
			//cut off # part first
			String r;
			int hash = s.indexOf("#");
			if(hash > 0) {
				//has # so keep everything but this
				r = s.substring(0, hash);

				//if url is empty now, discard (#abc type link)
				if(r.length() == 0) {
					continue;
				}
			}

			//parse into string array
			String[] splitURL = URLParser.parseURL(s);
			//[0] is http: or https:
			//[1] is between // and first / after host name:port aka it's the host name
			//[2] is port number, if present
			//[3] is "/" or rest of url after [2] or entire url if 0-2 are null
			//			for(String str : splitURL) {
			//				System.out.println("splitURL pieces" + str);
			//			}

			//check if relative link, if so, convert to absolute
			if(splitURL[0] == null && splitURL[1] == null && splitURL[2] == null) {
				//everything is in [3], no http://host:port in url so add base page info
				splitURL[0] = baseSplit[0];
				splitURL[1] = baseSplit[1];
				splitURL[2] = baseSplit[2];

				//link has starting slash so append to protocol, host and port of base url
				if(splitURL[3].startsWith("/")) {
					//TODO do i change 3 at all?
				}else if(splitURL[3].startsWith("../")) {
					logger.debug("url starts with ../");
					String base3 = baseSplit[3];
					while(splitURL[3].startsWith("../")) {
						//remove everything after last / in base url
						int slash = base3.lastIndexOf("/");
						if(slash > 0 && (slash) < base3.length()) {
							base3 = base3.substring(0, slash+1);
							//find / before
							int slash2 =  base3.lastIndexOf("/");
							if(slash2 >= 0) {
								base3 = base3.substring(0, slash+1);
							}
						}else if(slash == 0) {
							base3 = base3.substring(0, 1);
						}
						splitURL[3] = splitURL[3].substring(3);
					}
					//concatenate base + splitURL
					splitURL[3] = base3 + splitURL[3];
				}else {
					//link has no starting / or ..
					//replace what's after last/ in base url[3] with splitURL[3]
					String base3 = baseSplit[3];
					String sub;
					if(base3 != null) {
						//keep up to last slash in base url
						int slash = base3.lastIndexOf("/");
						if(slash > 0) {
							sub = base3.substring(0, slash+1);
							splitURL[3] = sub + splitURL[3];
						}else if(slash == 0) {
							sub = base3.substring(0, 1);
							splitURL[3] = sub + splitURL[3];
						}
					}
				}
			}else {
				//absolute, check for port #
				if(splitURL[2] == null) {
					if(splitURL[0].toLowerCase().equals("http")) {
						splitURL[2] = "80";
					}else if(splitURL[0].toLowerCase().equals("https")) {
						splitURL[2] = "443";
					}
				}
			}


			String normalizedUrl = splitURL[0] + "://" + splitURL[1]  + ":" + splitURL[2] + splitURL[3];
			//			System.out.println("normalizedUrl: " + normalizedUrl);

			//check if url should be filtered out
			//not http or https

			if((!splitURL[0].toLowerCase().equals("http")) && (!splitURL[0].toLowerCase().equals("https"))) {
				//				System.out.println("no start with http/s");
				continue;
			}

			boolean badExtension = false;
			//can't end in .jpg, .jpeg, .gif, .png, .txt
			String[] disallowedExtensions = {".jpg", ".jpeg", ".gif", ".png", ".txt"};
			for(String ext : disallowedExtensions) {
				if(normalizedUrl.toLowerCase().endsWith(ext)) {
					//					System.out.println("disallowed extension");
					badExtension = true;
					//					continue;//this only applies to this for loop, not the main loop
					break;
				}
			}

			if(badExtension) {
				continue;
			}else {
				//else add to normalized arrayList to return
				normalizedArray.add(normalizedUrl);
			}
		}

		//		System.out.println("size of normalizedArray: " + normalizedArray.size());

		return normalizedArray;
	}

	//-----------------------RUN METHOD--------------------------------------

	public static void run(FlameContext fc, String[] args) throws Exception {
		if(args.length == 1) {
			fc.output("OK");
		}else {
			throw new Exception("Syntax: <convergence threshold>");
			//			fc.output("Syntax: <convergence threshold>");
		}

		//		System.out.println("threshold: " + args[0]);

		double convergenceThreshold = Double.parseDouble(args[0]);


		//-----------load data from pt-crawl table into FlamePairRDD-----------------

		//use FlameContext.fromTable
		//row is a Row object
		FlameRDD rdd = fc.fromTable("pt-crawl", row -> {
			Row r = row;
			String url = r.get("url");
			byte[] page = r.getBytes("page");//can this be null? - i think so since still create row for urls returning error codes or redirects

			if(page == null) {
				return null;
			}

			StringBuilder sb = new StringBuilder();
			sb.append(url);
			sb.append(",");
			String pageString = new String(page);
			sb.append(pageString);
			return sb.toString();
		});



		//use mapToPair on it to convert to a PairRDD
		//s is url,page string
		//create (u,"1.0,1.0,L") FlamePair where u is hash of the url and L is list of normalized urls on page
		//----------STATE TABLE IN HANDOUT------------------- STEP #3 END
		FlamePairRDD pairRdd = rdd.mapToPair(s -> {

			//Split it so we have url, page -> split into 2 pieces on first ","
			try {
				String[] parts = s.split(",", 2);
				String[] urlparts = URLParser.parseURL(parts[0]);

				String urlBase = urlparts[0] + "://" + urlparts[1] + ":" + urlparts[2];

				String u = parts[0];

				//create L - use hw8 for extracting and normalizing links
				ArrayList<String> extractedUrlList = extractURL(parts[1]);
				for (String str : extractedUrlList) {
					logger.info("extractedUrlList: " + str);
				}


			ArrayList<String> normalizedUrlList = normalizeAndFilterURL(extractedUrlList, urlBase);
			
			//TODO remove duplicates
			HashSet<String> uniqueUrlList = new HashSet<String>(normalizedUrlList); 
			
			

				//create comma separated list from the arrayList
				StringBuilder list = new StringBuilder();
//			for(String str : normalizedUrlList) {
//				logger.info("normalizedUrlList: " + str);
				for (String str : uniqueUrlList) {
					logger.info("uniqueUrlList: " + str);
					if (str.length() > 0) {
						list.append(Hasher.hash(str));
						list.append(",");
					}
				}

				//			System.out.println("list length: " + list.length());

				//for pages with links on them
				if (list.length() > 0) {
					list.deleteCharAt((list.length() - 1)); //extra comma at end
				}


				//return new FlamePair(u,"1.0,1.0," + normalizedUrlList.toString());
				//				if(list.toString().length() > 0) {

				//TODO add something to exclude list.toString if it's empty------------------xcvhjkdtgehdfsh 57y65serseruhje 5thute5 yheszd------------------------------------------------
				return new FlamePair(u, "1.0,1.0," + list.toString());
				//				return new FlamePair(parts[0], "1.0,1.0," + list.toString());
				//				}
				//				return null;
			} catch (Exception e) {
				logger.info("mapping error in second step. returning null");
				return null;
			}
		});

		//for debugging
		//		List<FlamePair> output = pairRdd.collect();
		//		for (FlamePair tuple : output)
		//			fc.output(tuple._1()+": "+tuple._2()+"\n");
		//			System.out.println(tuple._1()+": "+tuple._2()+"\n");



		//-----------------START LOOP HERE------------------------------------//


		//TESTING NEW START OF LOOP
		int loop = 0;
//		for(int loop1 = 0; loop1 < 1; loop1++) {	//FOR DEBUGGING SINGLE LOOP
		while(true) {
			loop++;
			//-----------COMPUTE TRANSFER TABLE--------------STEP #4
			FlamePairRDD transferTable = pairRdd.flatMapToPair(p -> {
				try {
					FlamePair pair = p;
					String urlName = pair._1();
					String string = pair._2();//"rc,rp,L" where L is list of urls

					//make string of (li, v) pairs - one for each link in L
					//v=0.85 * (rc/n) where n is number of links in L

					String[] a = string.split(",", 3);
					String currRank = a[0];
					logger.debug("PageRank- url: " + urlName + "current rank string : " + currRank);
					//				String prevRank = a[1];
					logger.debug("PageRank- url: " + urlName + " a[2]: " + a[2] + " a[2] length: " + a[2].length());
					String[] urlList = a[2].split(","); //split L on commas
					logger.debug("PageRank- url: " + urlName + " urlList length: " + urlList.length);


					ArrayList<FlamePair> pairList = new ArrayList<FlamePair>();

					//page has at least 1 link
					if (a[2].length() > 0) {
						//loop through urlList and create (li, v) pair for each
						for (String u : urlList) {
							logger.debug("PageRank-url: " + urlName + " urlList item u: " + u);
							//calculate v
							double urlListDouble = urlList.length;
							//					double v = 0.85 * (Double.parseDouble(currRank)/ urlList.length);
							double v = 0.85 * (Double.parseDouble(currRank) / urlListDouble);

							FlamePair fp = new FlamePair(Hasher.hash(u), String.valueOf(v));
							//				System.out.println(fp._1() + " ; " + fp._2());
							pairList.add(fp);
						}
					}

					//send rank 0 to each vertex those with indegree zero from disappearing
					pairList.add(new FlamePair(urlName, "0.0"));

					//for debugging
					//			for(FlamePair f : pairList) {
					//				System.out.println("urlName: " + urlName + " - "  + f._1() + " ; " + f._2());
					//			}

					//return Iterable<FlamePair>
					return pairList;
				} catch (Exception e) {
					logger.info("Error in computing transfer table");
					return null;
				}
			});


			//aggregate the transfers table 4
			FlamePairRDD aggregate = transferTable.foldByKey("0", (String a, String b) ->{

				try {

					//compute new rank for each page by adding up all the vi for the page u
					double a1 = Double.parseDouble(a);
					double b2 = Double.parseDouble(b);
					double sum = a1 + b2;

					return String.valueOf(sum);
				} catch (Exception e) {
					logger.info("Error in aggregation step. Returning null.");
					return null;
				}
			});

			//for debugging

			List<FlamePair> output = aggregate.collect();
			for (FlamePair tuple : output)
				logger.info(tuple._1()+": "+tuple._2()+"\n");
			//			System.out.println(tuple._1()+": "+tuple._2()+"\n");


			//----------Update the state table---------------

			//join old state table and aggregrated table table 5
			FlamePairRDD joinedTable = pairRdd.join(aggregate);

			//		//for debugging
			//		List<FlamePair> output = aggregate.collect();
			//		for (FlamePair tuple : output)
			//			System.out.println(tuple._1()+": "+tuple._2()+"\n");

			//throws out the old “previous rank” entry,
			//moves the old “current rank” entry to the new “previous rank” entry,
			//and moves the newly computed aggregate to the “current rank” entry
			//add 0.15 from the rank source
			//table 6
			FlamePairRDD modifiedJoinTable = joinedTable.flatMapToPair(p -> {
				try {

					//throws out the old “previous rank” entry,
					//moves the old “current rank” entry to the new “previous rank” entry,
					//and moves the newly computed aggregate to the “current rank” entry

					//for entry (a,b), b is (old cr, old pr, L, new cr)
					FlamePair pair = p;
					String urlName = pair._1();
					String string = pair._2(); //(old cr, old pr, L, new cr)
					String[] valueArray = string.split(",");
					String newPrevRank = valueArray[0];

					//new computed aggregate is last value in valueArray
					int lastIndex = valueArray.length - 1;
					String newCurrRank = valueArray[lastIndex];

					//add 0.15 from the rank source
					double currRank = Double.parseDouble(newCurrRank);
					currRank += 0.15;

					StringBuilder sb = new StringBuilder();

					//extract L - valueArray [2 through length-2)
					for (int i = 2; i < valueArray.length - 1; i++) {
						sb.append(valueArray[i]);
						if (i != valueArray.length - 2) {
							sb.append(",");
						}
					}

					String l = sb.toString();

					String fpValue = String.valueOf(currRank) + "," + newPrevRank + "," + l;

					ArrayList<FlamePair> pairList = new ArrayList<FlamePair>();

					//make new FlamePair and add to pairList
					FlamePair fp = new FlamePair(urlName, fpValue);

					pairList.add(fp);

					//return Iterable<FlamePair>
					return pairList;
				} catch (Exception e) {
					logger.info("Error in modifying join table. Returning null");
					return null;
				}
			});


			//for debugging
			//					List<FlamePair> output = modifiedJoinTable.collect();
			//					for (FlamePair tuple : output)
			//						System.out.println("modifiedJoinTable: " + tuple._1()+": "+tuple._2()+"\n");


			//---------------BOTTOM OF LOOP-------------------//

			//replace old state table with new one (first in loop replaced by last in loop)
			pairRdd = modifiedJoinTable;

			//compute max change in ranks across all pages - use flatMap - table 7
			FlameRDD computed = pairRdd.flatMap(p -> {
				try {

					//compute abs difference b/t old and current rank for page
					FlamePair pair = p;
					//				String urlName = pair._1();
					String string = pair._2();//"rc,rp,L" where L is list of urls

					String[] rankValues = string.split(",");
					double currRank = Double.parseDouble(rankValues[0]);
					double prevRank = Double.parseDouble(rankValues[1]);

					double difference = currRank - prevRank;
					difference = Math.abs(difference);

					//return Iterable<String> like List
					ArrayList<String> diff = new ArrayList<String>();
					diff.add(String.valueOf(difference));
					return diff;
				} catch (Exception e) {
					logger.info("Error in computing max change. Returning null");
					return null;
				}
			});

			//follow with fold that computes maximum of all page rank differences - table 8
			String maxPageRankDifference = computed.fold("0", (String a, String b) -> {
				try {

					double a1 = Double.parseDouble(a);
					double b1 = Double.parseDouble(b);
					logger.debug("pageRank fold- a1: " + a1 + " b1: " + b1);
					if ((a1 > b1) || (a1 == b1)) {
						return String.valueOf(a1);
					} else {
						return String.valueOf(b1);
					}
				} catch (Exception e) {
					logger.info("Error in max page rank difference step. Returning null.");
					return null;

				}

			});

			logger.debug("max difference: " + maxPageRankDifference);

			//if max is < convergence threshold, exit loop
			if(Double.parseDouble(maxPageRankDifference) < convergenceThreshold) {
				//----------SAVE THE RESULTS---------------------------
				//				System.out.println("Save the results");
				//convert data to format section 2 requires

				//run flatmaptopair over state table
				FlamePairRDD savingTable = pairRdd.flatMapToPair(p -> {
					try {
						//					System.out.println("inside savingTable");
						//decode each row
						FlamePair pair = p;
						String urlName = pair._1();//hashed url
						//					System.out.println("urlName: " + urlName);
						String string = pair._2();//"rc,rp,L" where L is list of urls
						//					System.out.println("string: " + string);
						String[] vals = string.split(",", 3);
						//					System.out.println("vals: " + vals);
						String currRank = vals[0];
						//					System.out.println("currRank: " + currRank);

						//put rank into pt-pageranks table in KVS
						//URLhash as the key, rank as column name
						KVSClient kvs = fc.getKVS();
						kvs.put("pt-pageranks", urlName, "rank", currRank);
						//					System.out.println("after kvs.put");

						//return empty list
						ArrayList<FlamePair> alfp = new ArrayList<FlamePair>();
						//					System.out.println("after creating alfp");
						return alfp;
					} catch (Exception e) {
						logger.info("Error in final saving to table. Returning null");
						return null;
					}
				});

				//				System.out.println("loop: " + loop);
				logger.info("loop: " + loop);
				break;
			}

		}
		//		System.out.println("outside of loop");


	}

}
