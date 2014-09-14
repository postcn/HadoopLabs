package edu.rosehulman.postcn.Lab1Friends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendListMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String friend = value.toString().charAt(0) + "";
		String[] friendList = value.toString().substring(2).split(",");
		for (String buddy : friendList) {
			String newKey = "(" + (friend.compareTo(buddy) < 0 ? friend + "," + buddy : buddy + "," + friend) + ")";
			String newValue = Arrays.toString(friendList).replace(buddy + ", ", "").replace(", " + buddy + "]", "]");
			context.write(new Text(newKey), new Text(newValue));
		}
	}

}
