import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WikiLangReducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
	/*
	 * Result used to keep track of the total page views for calculating the avg / lang over the period.
	 */
	private FloatWritable result = new FloatWritable(); 
	Float average = 0f; 
	Float count = 0f; 
	int sum = 0; 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException { 
		/*
		 * File writer used to crate logging per class.
		 */
		FileWriter writer = new FileWriter("//home//soc//langreducer_log.txt", true);
		
		/*
		 * Iterates the values list and sums the total page views
		 */
		for (IntWritable val : values) { 
			sum += val.get(); 
			} 
		count += 1;
		//Calculates the average page views.
		average = sum/count; 		
		result.set(average); 
		
		/*
		 * Writes the key and average to the log
		 */
		writer.write("Mapping: " + key.toString()  + result  +"\n");
		writer.close();
		
		/*
		 * Writes the Key and result to the context.
		 */
		context.write(key, result); 
	} 
}
