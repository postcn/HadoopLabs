package edu.rosehulman.postcn.Lab2SprintHours;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SprintDetailsReducer extends
		Reducer<SprintDetails, FloatWritable, SprintDetails, FloatWritable> {

	@Override
	protected void reduce(
			SprintDetails key,
			Iterable<FloatWritable> values,
			Reducer<SprintDetails, FloatWritable, SprintDetails, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		float tot = 0;
		for (FloatWritable val : values) {
			tot += val.get();
		}
		context.write(key, new FloatWritable(tot));
	}

}
