import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Combiner needed to do a pre map of the data latter used by the mapper to generate the average page views per#
 * language over the specified time period.
 */
public class WikiLangCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * Integer variable for the result of the combination process.
	 */
	private static IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		/*
		 * File writer used to crate logging per class.
		 */
		FileWriter writer = new FileWriter("//home//soc//combiner_log.txt", true);
		
		int count = 0;
		for(IntWritable value : values){
			count += value.get();
		}
		
		/*
		 * Sets the result to the value of count.
		 */
		result.set(count);
		
		/*
		 * Writes out the key, count and result value to the log file.
		 */
		writer.write("Combining: " + key.toString() + " " + count + " " + result  +"\n");
		writer.close();
		
		/*
		 * Writes the Key and result to the context for use in the mapper.
		 */
		context.write(key, result);
	}
}