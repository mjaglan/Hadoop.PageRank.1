package indiana.cgl.hadoop.pagerank.helper;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #1: Preprocessing the input for PHASE #2
 * 
 * MAPPER: Read each line of 'Adjacency List' record from the raw input file.
 * Give initial page-rank value to source node.
 * Emit the new format of 'Adjacency List' to REDUCER.
 * 
 * Input: Text object like this -
 * 		KEY: <file-offset>
 *		VALUE: "<source-id>   <out-bound-node>   <out-bound-node>   <out-bound-node>   <out-bound-node>   <out-bound-node>"
 * 
 * Output:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CreateGraphMap extends
Mapper<LongWritable, Text, LongWritable, Text> {

	// Rebuild Adjacency List in desired format!
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] strArray = value.toString().split(" ");

		int sourceUrl;
		sourceUrl = Integer.parseInt(strArray[0]);

		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		double initialWeight = 1.0 / numUrls; // initial <source-rank-value>

		StringBuffer stringBufferObj = new StringBuffer();
		stringBufferObj.append(String.valueOf(initialWeight));

		int targetUrl;
		for (int i = 1; i < strArray.length; i++) {
			targetUrl = Integer.parseInt(strArray[i]);
			stringBufferObj.append("#" + targetUrl);
		}

		context.write(new LongWritable(sourceUrl),
				new Text(stringBufferObj.toString()));

	}// map
}
