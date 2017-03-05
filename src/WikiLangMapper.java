import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiLangMapper  extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		/*
		 * File writer used to crate logging per class.
		 */
		FileWriter writer = new FileWriter("//home//soc//languagemapper_log.txt", true);

		/*
		 * Splits the value on white space and puts into a new string array.
		 */
		String[] valueArray = value.toString().split("\\s+");
		
		/*
		 * Writes the first entry in the array to the file.
		 * The first entry is the langauge code fo the record in the HDFS
		 */
		writer.write("Mapping: " + valueArray[0] + "\n");
		writer.close();
		
		/*
		 * Writes the key value paring of the language code and the number 1.
		 */
		context.write(new Text(valueArray[0].toLowerCase()), new IntWritable(1));
	}

}
