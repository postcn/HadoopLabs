package edu.rosehulman.postcn.Lab3ReduceSideJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntPair implements WritableComparable<IntPair> {
	private IntWritable first;
	private IntWritable second;
	
	public IntPair(int keyFirst, int keySecond) {
		first = new IntWritable(keyFirst);
		second = new IntWritable(keySecond);
	}
	
	public IntPair(IntWritable keyFirst, IntWritable keySecond) {
		first = keyFirst;
		second = keySecond;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	public int compareTo(IntPair other) {
		int comparator = this.first.compareTo(other.first);
		if (comparator != 0) {
			return comparator;
		}
		return this.second.compareTo(other.second);
	}
	
	public IntWritable getFirst() {
		return this.first;
	}

}
