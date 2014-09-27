package edu.rosehulman.postcn.Lab3ReduceSideJoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	
	static class JoinPartitioner extends Partitioner<IntPair, Text> {

		@Override
		public int getPartition(IntPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().get() * 127) % numPartitions;
		}
		
	}
	
	static class JoinComparator extends WritableComparator {
		public JoinComparator() {
			super(IntPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair p1 = (IntPair) a;
			IntPair p2 = (IntPair) b;
			
			return p1.compareTo(p2);
		}
		
	}
	
	static class JoinGroupComparator extends WritableComparator {
		public JoinGroupComparator() {
			super(IntPair.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntPair p1 = (IntPair) a;
			IntPair p2 = (IntPair) b;
			
			return p1.getFirst().compareTo(p2.getFirst());
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: JoinRunner <input sprints> <input users> <output>");
			System.exit(-1);
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(JoinRunner.class);
		job.setJobName("Join Runner");
		
		Path sprintPath = new Path(args[0]);
		Path userPath = new Path(args[1]);
		
		MultipleInputs.addInputPath(job, sprintPath, TextInputFormat.class, JoinSprintMapper.class);
		MultipleInputs.addInputPath(job, userPath, TextInputFormat.class, JoinWorkMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(JoinPartitioner.class);
		job.setSortComparatorClass(JoinComparator.class);
		job.setGroupingComparatorClass(JoinGroupComparator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinRunner(), args);
		System.exit(exitCode);
	}

}
