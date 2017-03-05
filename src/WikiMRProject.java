import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WikiMRProject {
	
	//Presets the folder and file path to a default. Assumes this is the only files to be read by MR process
	private static String inputPath = "pagecount//pagecounts-20160101-";
	private static String outputFolder = "";
	private static int numHours = 0;
	private static int fromHour = 0;
	private static int toHour = 0;
	
	
	public static void main(String[] args) throws Exception {
		/*
		 * Check args are valid and exits with error meassge if fails.
		 * If valid args then proceeds with MR process.
		 */
		if(!validateArgs(args)){
			System.exit(-1);
		}
		
		/*
		 * File writer used to crate logging per class.
		 */
		FileWriter writer = new FileWriter("//home//soc//driver_log.txt", true);
		
		/*
		 * Ouput folder is set to be the user defined output + the from and to hour logs
		 */
		outputFolder = outputFolder + args[2] + "_" + args[0] + "-" + args[1];
		
		/*
		 * Log the number of hours being parsed and the output folder location.
		 */
		writer.write("Hours to parse: " + numHours);
		writer.write("Outputing to: " + outputFolder);
		
		/*
		 * Check to ensure the job succeeded and if not stops the MR Driver and notifies of the failure.
		 * If success then continues on to the next process.
		 * Logs success or failure in driver log and writes to console.
		 */
		if(runAvgLangOverPeriod(writer) == false){
			writer.write("Job Failed");
			System.err.println("Job Failed");
			writer.close();
			System.exit(-1);
		}else{
			writer.write("Job Successful -> Continueing");
			System.out.println("Job Successful -> Continueing");
		}
		if(runTotalPageViews(writer) == false){
			writer.write("Job Failed");
			System.err.println("Job Failed");
			writer.close();
			System.exit(-1);
		}else{
			writer.write("Job Successful -> Continueing");
			System.out.println("Job Successful -> Continueing");
		}		
		
		/*
		 * Close writer and exit MR Driver if all is successful.
		 */
		writer.close();
		System.exit(1);
	}

	private static boolean runTotalPageViews(FileWriter writer) throws Exception{
		/*
		 * Creates a configuration to be used by the job for.
		 * Created to allow use of the MultipleInputs class.
		 */
		Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
		
        /*
         * Creates job and sets name and config
         */
		Job job = new Job(conf, "TotalPageViews");
		job.setJarByClass(WikiMRProject.class);
		job.setJobName("TotalPageViews");
		
		//Writes to log that the job is beginning
		writer.write("Starting Job: " + job.getJobName());
		
		/*
		 * Defines mapper, reducer, and comparator.
		 */
		job.setMapperClass(WikiPageViewsMapper.class);
		job.setReducerClass(WikiLangCombiner.class);
		job.setSortComparatorClass(DecreasingComparator.class);
		
		/*
		 * Sets output key and value types
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * Allows for input of any files contained within the HDFS.
		 * Sets up multiple inputs by adding the index (ix) to the fromHour.
		 * Index is incremented for the total duration of hours to be scanned.
		 * ex. 
		 * 		fromHour = 02
		 * 		toHour = 05
		 * 		numHours = 3
		 * 		02 + 0 = 02, 02 + 1 = 03, 02 + 2 = 04, 02 + 3 = 05
		 */
		for(int ix = 0; ix <= numHours; ix++){
			String inputFile = inputPath + fromHour + ix + "0000";
			MultipleInputs.addInputPath(job, new Path(inputFile), TextInputFormat.class);	
		}
		
		/*
		 * Sets file output path for MR process to the user args + the job name.
		 */
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + "_" + job.getJobName()));
		
		/*
		 * returns true when job succeeds and false when fails
		 */
		return job.waitForCompletion(true);
	}

	private static boolean runAvgLangOverPeriod(FileWriter writer) throws Exception {
		/*
		 * Creates a configuration to be used by the job for.
		 * Created to allow use of the MultipleInputs class.
		 */
		Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
		
        /*
         * Creates job and sets name and config
         */
		Job job = new Job(conf, "AvgPagePerLang");
		job.setJarByClass(WikiMRProject.class);
		job.setJobName("AvgPagePerLang");
		
		//Writes to log that the job is beginning
		writer.write("Starting Job: " + job.getJobName());
		
		/*
		 * Defines mapper, reducer, and comparator.
		 */
		job.setMapperClass(WikiLangMapper.class);
		job.setCombinerClass(WikiLangCombiner.class);
		job.setReducerClass(WikiLangReducer.class);
		job.setSortComparatorClass(DecreasingComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * Allows for input of any files contained within the HDFS.
		 * Sets up multiple inputs by adding the index (ix) to the fromHour.
		 * Index is incremented for the total duration of hours to be scanned.
		 * ex. 
		 * 		fromHour = 02
		 * 		toHour = 05
		 * 		numHours = 3
		 * 		02 + 0 = 02, 02 + 1 = 03, 02 + 2 = 04, 02 + 3 = 05
		 */
		for(int ix = 0; ix <= numHours; ix++){
			String inputFile = inputPath + fromHour + ix + "0000";
			MultipleInputs.addInputPath(job, new Path(inputFile), TextInputFormat.class);	
		}
		
		/*
		 * Sets file output path for MR process to the user args + the job name.
		 */
		FileOutputFormat.setOutputPath(job, new Path(outputFolder + job.getJobName()));
		/*
		 * returns true when job succeeds and false when fails
		 */
		return job.waitForCompletion(true);
	}

	private static boolean validateArgs(String[] args) {
		/*
		 * Expects 3 arguments
		 * <from> <to> <output folder>
		 * Fails validation and exits with error message
		 */
		if(args.length != 3){
			System.err.println("Error in Arguments \n");
			System.err.println("Expected <from> <to> <ouput folder>");
			return false;
		}
		
		/*
		 * Input for hours should be 2 digits/chars ie. 00, 01, etc.
		 * Fails validation and exits with error message
		 */
		if(args[0].length() != 2 || args[1].length() != 2){
			System.err.println("Error in Arguments \n");
			System.err.println("Hour should be in format ie. 00");
			return false;
		}
		
		/*
		 *Converts input hours to integers and gets the difference 
		 */
		fromHour = Integer.valueOf(args[0]);
		toHour = Integer.valueOf(args[1]);
		numHours = toHour - fromHour;
		
		/*
		 * Checks to see that from < to
		 * Fails validation and exits with error message
		 */
		if(fromHour > toHour){
			System.err.println("Error in Arguments \n");
			System.err.println("From > To");
			return false;
		}
		
		/*
		 * Checks that hours are between 00 and 23 to cover a 24 hour range.
		 * Fails validation and exits with error message
		 */
		if(0 > fromHour || fromHour > 23 || 0 >  toHour || toHour > 23){
			System.err.println("Error in Arguments \n");
			System.err.println("Hour should be 00 < X < 23 ");
			return false;
		}
		
		/*
		 * Minimum of 2 files have to be scanned.
		 * from - to >= 1
		 * ex. 01 - 00 = 1 -> pass
		 * ex. 16 - 10 = 6 -> pass
		 * ex. 22 - 22 = 0 -> fail
		 * Fails validation and exits with error message
		 */
		if(numHours < 1){
			return false;
		}
		/*
		 * Arguments are accepted and application proceeds.
		 * Returns true for success
		 */
		System.out.println("Arguments accepted.");
		System.out.println("MR process started");		
		return true;
	}
}
