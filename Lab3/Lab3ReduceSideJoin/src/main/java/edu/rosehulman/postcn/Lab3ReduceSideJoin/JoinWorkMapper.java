package edu.rosehulman.postcn.Lab3ReduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinWorkMapper extends Mapper<LongWritable, Text, IntPair, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntPair, Text>.Context context)
			throws IOException, InterruptedException {
		String[] parsed = value.toString().split(",");
		int id = Integer.parseInt(parsed[2]);
		context.write(new IntPair(id, JoinTag.WORKER_TAG.getValue()), new Text(parsed[0] + " " + parsed[1]));
	}

}
