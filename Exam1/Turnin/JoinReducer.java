package edu.rosehulman.postcn.Exam1Postcn;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextInt, Text, NullWritable, Text> {

	@Override
	protected void reduce(TextInt key, Iterable<Text> values,
			Reducer<TextInt, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		String courseName = iter.next().toString();
		String courseID = key.getFirst().toString();
		while(iter.hasNext()) {
			String[] elements = iter.next().toString().split(",");
			String person = elements[0];
			String score = elements[1];
			context.write(null, new Text(person + "\t" +courseID +"\t" +courseName+ "\t"+ score));
		}
	}

}
