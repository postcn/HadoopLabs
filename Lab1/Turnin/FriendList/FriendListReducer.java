package edu.rosehulman.postcn.Lab1Friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendListReducer extends
		Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		ArrayList<String> commonFriends = new ArrayList<String>();
		Iterator<Text> iter = values.iterator();
		String first = iter.next().toString();
		String[] firstFriendsList = first.replace("[", "").replace("]", "").split(",");
		for (String friend: firstFriendsList) {
			commonFriends.add(friend);
		}
		while(iter.hasNext()) {
			ArrayList<String> nextFriendsList = new ArrayList<String>(Arrays.asList(iter.next().toString().replace("[", "").replace("]", "").split(",")));
			ArrayList<String> missing = new ArrayList<String>();
			for (String friend : commonFriends) {
				if (!nextFriendsList.contains(friend)) {
					missing.add(friend);
				}
			}
			commonFriends.removeAll(missing);
		}
		context.write(key,new Text(commonFriends.toString()));
	}

}
