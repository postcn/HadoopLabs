package edu.rosehulman.postcn.Exam1Postcn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, TextInt, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, TextInt, Text>.Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String name = values[0];
		String course = values[1];
		String score = values[2];
		
		String writer = name+","+score;
		
		context.write(new TextInt(course, JoinTag.STUDENT_TAG.getValue()), new Text(writer));
	}

}
