package indiana.cgl.hadoop.pagerank;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #2: Computing the page-rank values of each node using iterative map-reduce programming model.
 * 
 * REDUCER: For each source-node re-calculate the page-rank-value based on in-bound contributions received from other nodes!
 * 
 * Input:
 * 		KEY: <source-id>
 * 
 *		VALUE-LIST: [value-type-1], [value-type-2]
 * 
 * 			[value-type-1]:
 * 				"<rank-value-contribution-received-from-other-nodes>"
 * 
 * 			[value-type-2]:
 * 				"###<source-id>   <source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 * 
 * 
 * Output:
 * 		KEY: <source-id>
 *		VALUE: "<NEW-source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends
Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		double sumOfRankValues = 0.0;
		StringBuffer targetUrlsList = new StringBuffer("");

		int numUrls = context.getConfiguration().getInt("numUrls", 1);

		StringBuffer loggingString = new StringBuffer("");

		// Each entry may include either rank value or adjacency-list-record
		for (Text value : values) {

			String line = value.toString();
			loggingString.append(line);
			loggingString.append(",  ");

			if (line.contains("###")) {
				String normal = line.substring(3);
				RankRecord rrd = new RankRecord(normal);

				for (Integer eachTarget : rrd.targetUrlsList) {
					targetUrlsList.append("#");
					targetUrlsList.append(eachTarget);
				}

			} else {

				try {
					sumOfRankValues += Double.parseDouble(value.toString());

				} catch (NumberFormatException nfe) {
					sumOfRankValues += 0.0;
					System.out.println("REDUCER WARN: bad rank values encountered!");
				}

			}

		}
		// System.out.println("REDUCER INPUT    <key>: "+key.toString()+"    <values>: "+logString.toString());

		sumOfRankValues = (0.85 * sumOfRankValues + 0.15 * (1.0))  /  numUrls; // calculate using the formula
		// System.out.println("REDUCER OUTPUT    <key>: "+key.toString()+"    <value>: "+sumOfRankValues+targetUrlsList.toString());
		context.write(key, new Text(sumOfRankValues + targetUrlsList.toString()));

	} // reduce

}
