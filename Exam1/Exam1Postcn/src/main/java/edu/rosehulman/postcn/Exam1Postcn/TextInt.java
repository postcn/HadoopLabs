package edu.rosehulman.postcn.Exam1Postcn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextInt implements WritableComparable<TextInt> {
	
	private Text first;
	private IntWritable second;
	
	public TextInt() {
		first = new Text();
		second = new IntWritable();
	}
	
	public TextInt(String arg0, int arg1) {
		first = new Text(arg0);
		second = new IntWritable(arg1);
	}
	
	public Text getFirst() {
		return first;
	}
	
	public IntWritable getSecond() {
		return second;
	}
	
	public TextInt(Text arg0, IntWritable arg1) {
		first = arg0;
		second = arg1;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public int compareTo(TextInt arg0) {
		int comp = first.compareTo(arg0.first);
		if (comp != 0) {
			return comp;
		}
		return second.compareTo(arg0.second);
	}

}
