package edu.rosehulman.postcn.Lab2SprintHours;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class SprintDetails implements WritableComparable<SprintDetails> {
	
	private Text firstName;
	private Text lastName;
	private IntWritable sprintID;
	
	public SprintDetails() {
		set(new Text(), new Text(), new IntWritable());
	}
	
	public SprintDetails(String first, String last, int id) {
		set(new Text(first), new Text(last), new IntWritable(id));
	}
	
	public SprintDetails(Text first, Text last, IntWritable id) {
		set(first, last, id);
	}
	
	public void set(Text first, Text last, IntWritable id) {
		this.firstName = first;
		this.lastName = last;
		this.sprintID = id;
	}

	public void readFields(DataInput arg0) throws IOException {
		firstName.readFields(arg0);
		lastName.readFields(arg0);
		sprintID.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		firstName.write(arg0);
		lastName.write(arg0);
		sprintID.write(arg0);
	}
	
	@Override
	public String toString() {
		return this.firstName +"\t" + this.lastName + "\t" + this.sprintID;
	}

	public int compareTo(SprintDetails arg0) {
		int cmp = firstName.compareTo(arg0.firstName);
	    if (cmp != 0) {
	      return cmp;
	    }
	    cmp = lastName.compareTo(arg0.lastName);
	    if (cmp != 0) {
	    	return cmp;
	    }
	    return sprintID.compareTo(arg0.sprintID);
	}

}
