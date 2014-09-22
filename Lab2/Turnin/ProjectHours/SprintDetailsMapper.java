package edu.rosehulman.postcn.Lab2SprintHours;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SprintDetailsMapper extends
		Mapper<LongWritable, Text, SprintDetails, FloatWritable> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, SprintDetails, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		String[] vals = value.toString().split(",");
		String firstName = vals[0];
		String lastName = vals[1];
		int id = Integer.parseInt(vals[2]);
		float time = Float.parseFloat(vals[3]);
		
		context.write(new SprintDetails(firstName, lastName, id), new FloatWritable(time));
	}

}
