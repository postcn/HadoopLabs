package edu.rosehulman.postcn.Lab3ReduceSideJoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<IntPair, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntPair arg0, Iterable<Text> arg1,
			Reducer<IntPair, Text, IntWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		Iterator<Text> iter = arg1.iterator();
		String name = iter.next().toString();
		while(iter.hasNext()) {
			String person = iter.next().toString();
			arg2.write(arg0.getFirst(), new Text(name + "\t" + person));
		}
		
	}

}
