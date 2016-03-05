package indiana.cgl.hadoop.pagerank.helper;

/*
 * CSCI B649 Cloud Computing
 * 
 * PHASE #1: Preprocessing the input for PHASE #2
 * 
 * REDUCER: Just emit results of MAPPER.
 * 
 * Input:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 * 
 * Output:
 * 		KEY: <source-id>
 *		VALUE: "<source-rank-value>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>#<out-bound-node>"
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CreateGraphReduce extends
Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException {

		try {

			Text outputValue = values.iterator().next();
			context.write(key, outputValue);

			// Log the value of reduce
			// Log log = LogFactory.getLog(HadoopPageRank.class);
			// log.info("values.iterator().next() = " + outputValue);
			System.out.println("values.iterator().next() = " + outputValue);

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
