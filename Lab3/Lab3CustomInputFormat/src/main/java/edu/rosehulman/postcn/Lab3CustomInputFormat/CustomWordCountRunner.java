package edu.rosehulman.postcn.Lab3CustomInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomWordCountRunner extends Configured implements Tool {
	public static final String SEARCH_KEY = "SearchString";
	
	enum Twos {
		EQUAL_TO_TWO,
		LESS_THAN_TWO,
		GREATER_THAN_TWO
	}
	
	static class CustomWordCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void map(Text key, IntWritable value,
				Mapper<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
			
			int val = value.get();
			if (val == 2) {
				context.getCounter(Twos.EQUAL_TO_TWO).increment(1);
			}
			else if (val < 2) {
				context.getCounter(Twos.LESS_THAN_TWO).increment(1);
			}
			else if (val > 2) {
				context.getCounter(Twos.GREATER_THAN_TWO).increment(1);
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: CustomWordCountRunner <input> <output> <search value>");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(CustomWordCountRunner.class);
		job.setJobName("Custom Word Count Runner");
		
		job.setInputFormatClass(CustomWordCountInputFormat.class);
		job.setMapperClass(CustomWordCountMapper.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Configuration conf = job.getConfiguration();
		conf.set(SEARCH_KEY, args[2]);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		int retVal = job.waitForCompletion(true) ? 0 : 1;
		
		if (!job.isComplete()) {
			System.err.println("Job is not complete but counters were attempted to be printed");
			System.exit(-1);
		}
		
		Counters counters = job.getCounters();
		long equal = counters.findCounter(Twos.EQUAL_TO_TWO).getValue();
		long less = counters.findCounter(Twos.LESS_THAN_TWO).getValue();
		long greater = counters.findCounter(Twos.EQUAL_TO_TWO).getValue();
		System.out.println("Outputting results of the counters.");
		System.out.printf("%d were equal to Two. %d were greater than Two. %d were less than Two.\n", equal, greater, less);
		long bytes = counters.findCounter(TaskCounter.MAP_OUTPUT_BYTES).getValue();
		System.out.printf("%d bytes were fed out of the mapper.\n", bytes);
		
		return retVal;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CustomWordCountRunner(), args);
		System.exit(exitCode);
	}

}
