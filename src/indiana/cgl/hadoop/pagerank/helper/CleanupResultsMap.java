package indiana.cgl.hadoop.pagerank.helper;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #3: Postprocessing the output of PHASE #2.
 * 
 * MAPPER: Collect the page rank results from PHASE #2 computation.
 * 
 * Input: Text object like this -
 * 		KEY: <file-offset>
 * 		VALUE:  "<source-id>	<source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 * 
 * Output:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>"
 */

import indiana.cgl.hadoop.pagerank.RankRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanupResultsMap extends
Mapper<LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String strLine = value.toString();
		RankRecord rrObj = new RankRecord(strLine);

		context.write(new LongWritable(rrObj.sourceUrl),
				new Text(String.valueOf(rrObj.rankValue)));
	}

}
