package edu.rosehulman.postcn.Exam1Postcn;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinRunner extends Configured implements Tool {
	
	static class JoinPartitioner extends Partitioner<TextInt, Text> {

		@Override
		public int getPartition(TextInt key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode() * 127) % numPartitions;
		}
		
	}
	
	static class JoinComparator extends WritableComparator {
		public JoinComparator() {
			super(TextInt.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextInt p1 = (TextInt) a;
			TextInt p2 = (TextInt) b;
			
			return p1.compareTo(p2);
		}
		
	}
	
	static class JoinGroupComparator extends WritableComparator {
		public JoinGroupComparator() {
			super(TextInt.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			TextInt p1 = (TextInt) a;
			TextInt p2 = (TextInt) b;
			
			return p1.getFirst().compareTo(p2.getFirst());
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: JoinRunner <input grades> <input courses> <output>");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(JoinRunner.class);
		job.setJobName("Join Runner");
		
		Path grades = new Path(args[0]);
		Path courses = new Path(args[1]);
		
		MultipleInputs.addInputPath(job, grades, TextInputFormat.class, StudentMapper.class);
		MultipleInputs.addInputPath(job, courses, TextInputFormat.class, ClassMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		job.setSortComparatorClass(JoinComparator.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		
		job.setMapOutputKeyClass(TextInt.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinRunner(), args);
		System.exit(exitCode);
	}

}
