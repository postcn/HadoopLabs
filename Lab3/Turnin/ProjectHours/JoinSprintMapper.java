package edu.rosehulman.postcn.Lab3ReduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSprintMapper extends Mapper<LongWritable, Text, IntPair, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntPair, Text>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		int sprintID = Integer.parseInt(split[0]);
		context.write(new IntPair(sprintID, JoinTag.SPRINT_TAG.getValue()), new Text(split[1]));
	}

}
