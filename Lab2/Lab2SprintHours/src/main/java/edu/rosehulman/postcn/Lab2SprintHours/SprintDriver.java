package edu.rosehulman.postcn.Lab2SprintHours;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SprintDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Sprint Hours Job");
		job.setJarByClass(SprintDriver.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setOutputKeyClass(SprintDetails.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setMapperClass(SprintDetailsMapper.class);
		job.setReducerClass(SprintDetailsReducer.class);
		
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SprintDriver(), args);
		System.exit(exitCode);
	}

}
