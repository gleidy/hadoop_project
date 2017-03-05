import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WikiPageViewsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	/*
	 * Regex used to validate the URI
	 */
	private static final String regex = "\\b[-A-Z0-9+&@#/%?=~_|!:,.;]*[-A-Z0-9+&@#/%=~_|]";
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		/*
		 * File writer used to crate logging per class.
		 */
		FileWriter writer = new FileWriter("//home//soc//pageviewmapper_log.txt", true);
		
		/*
		 * Splits the value on white space and puts into a new string array.
		 */
		String[] valueArray = value.toString().split("\\s+");	
		
		/*
		 * Checks that the URI is valid and then writes it to the log and then to context for use in the reducer.
		 */
		if(IsMatch(valueArray[1])){
			writer.write("Mapping: " + valueArray[1] + "\n");
			writer.close();
				
			context.write(new Text(valueArray[1]), new IntWritable(1));
		}
	}
	
    private static boolean IsMatch(String s) {
    	/*
    	 * Checks the URI against the regex to validate the URI and returns true or false accordingly.
    	 */
        try {
            Pattern patt = Pattern.compile(regex);
            Matcher matcher = patt.matcher(s);
            return matcher.matches();
        } catch (RuntimeException e) {
        	return false;
        }
    }
}
