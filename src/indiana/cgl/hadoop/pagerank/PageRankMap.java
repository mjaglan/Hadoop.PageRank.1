package indiana.cgl.hadoop.pagerank;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #2: Computing the page-rank values of each node using iterative map-reduce programming model.
 * 
 * MAPPER: Compute contribution of source-node to the page-rank-values of its outbound-nodes!
 * 
 * Input:
 * 		KEY: <file-offset>
 *		VALUE: "<source-id>   <source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 * 
 * Output 1:
 * 		KEY: <out-bound-node>
 *		VALUE: "<rank-value-contribution-to-out-bound-node>"
 *
 * Output 2:
 * 		KEY: <source-id>
 *		VALUE: "###<source-id>   <source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMap extends Mapper<LongWritable, Text, LongWritable, Text> {

	// each map task handles one record within the whole adjacency list
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		// System.out.println("MAP INPUT    <key>: " + key.toString() + "    <value>: " + line);

		int numUrls = context.getConfiguration().getInt("numUrls", 1);

		// instance an object that records the information for one webpage (aka node)
		RankRecord rrd = new RankRecord(line);

		if (rrd.targetUrlsList.size() <= 0) {
			// there is no out degree for this webpage!
			// scatter its rank value to all other urls

			// System.out.println("MAP ZERO SIZE source Url.: "+rrd.sourceUrl+"  Rank:"+rrd.rankValue+" Targetlist."+
			//					  rrd.targetUrlsList+ "list size :"+rrd.targetUrlsList.size());

			double rankValuePerUrl = rrd.rankValue / numUrls;
			// System.out.println("Source URL"+rrd.sourceUrl+"Rank :"+ rankValuePerUrl);

			for (int i = 0; i < numUrls; i++) {
				// System.out.println("MAP OUTPUT 1.1    <key>: " + i + "    <value>: " + rankValuePerUrl);
				context.write(new LongWritable(i), new Text(String.valueOf(rankValuePerUrl)));
			}

		} else {

			// System.out.println("MAP source Url.: "+rrd.sourceUrl+"  Rank:"+rrd.rankValue+" Targetlist."+
			//					  rrd.targetUrlsList+ "list size :"+rrd.targetUrlsList.size());

			double rankValuePerUrl = (rrd.rankValue / rrd.targetUrlsList.size());
			// System.out.println("Source URL"+rrd.sourceUrl+"Rank :"+ rankValuePerUrl);

			for (int val : rrd.targetUrlsList) {
				// System.out.println("MAP OUTPUT 1.2    <key>: " + val + "    <value>: " + rankValuePerUrl);
				context.write(new LongWritable(val), new Text(String.valueOf(rankValuePerUrl)));
			}

		}

		String mOut = "###" + line;
		// System.out.println("MAP OUTPUT 2.0    <key>: " + rrd.sourceUrl + "    <value>: " + mOut);
		context.write(new LongWritable(rrd.sourceUrl), new Text(mOut));

	} // end map

}
