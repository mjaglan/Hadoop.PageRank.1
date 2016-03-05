package indiana.cgl.hadoop.pagerank;

/*
 * CSCI B649 Cloud Computing
 * 
 * CUSTOM DATA STRUCTURE: Format each page-rank-adjacency-list record into a class object.
 * 
 * Input:
 *		"<source-id> <source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 * 
 * Output: A RankRecord Object With 3 member variables -
 * 		sourceUrl
 *		rankValue
 *		targetUrlsList
 */

import java.util.ArrayList;

public class RankRecord {

	public int sourceUrl; 	 // [0][0]
	public double rankValue; // [0][1]
	public ArrayList<Integer> targetUrlsList; // [1]...[N-1]

	public RankRecord(String strLine) {

		String[] strArray = strLine.split("#"); // "0 3.493238115915533" at [0]

		sourceUrl = Integer.parseInt(strArray[0].split("\t")[0]);
		rankValue = Double.parseDouble(strArray[0].split("\t")[1]);

		targetUrlsList = new ArrayList<Integer>();
		for (int i = 1; i < strArray.length; i++) {
			targetUrlsList.add(Integer.parseInt(strArray[i]));
		}

	}
}
