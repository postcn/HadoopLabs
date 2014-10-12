package edu.rosehulman.postcn.Exam1Postcn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClassMapper extends Mapper<LongWritable, Text, TextInt, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, TextInt, Text>.Context context)
			throws IOException, InterruptedException {
		String [] elements = value.toString().split(",");
		String id = elements[0];
		String name = elements[1];
		context.write(new TextInt(id, JoinTag.CLASS_TAG.getValue()), new Text(name));
	}

}
