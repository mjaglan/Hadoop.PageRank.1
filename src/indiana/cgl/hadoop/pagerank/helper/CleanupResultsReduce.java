package indiana.cgl.hadoop.pagerank.helper;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #3: Clean Up of results
 * 
 * REDUCER: Just emit results of MAPPER.
 * 
 * Input:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>"
 * 
 * Output:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>"
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupResultsReduce extends
Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		context.write(key, values.iterator().next());

	}

}