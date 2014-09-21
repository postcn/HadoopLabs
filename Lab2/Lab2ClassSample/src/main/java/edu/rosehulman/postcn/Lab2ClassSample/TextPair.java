package edu.rosehulman.postcn.Lab2ClassSample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	
	private Text firstName;
	private Text lastName;
	
	public TextPair() {
		set(new Text(), new Text());
	}
	
	public TextPair(String first, String last) {
		set(new Text(first), new Text(last));
	}
	
	public TextPair(Text first, Text last) {
		set(first, last);
	}
	
	public void set(Text first, Text last) {
		firstName = first;
		lastName = last;
	}
	
	public Text getFirst() {
		return this.firstName;
	}
	
	public Text getLast() {
		return this.lastName;
	}

	public void readFields(DataInput arg0) throws IOException {
		firstName.readFields(arg0);
		lastName.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		firstName.write(arg0);
		lastName.write(arg0);
	}
	
	public int hashCode() {
		return firstName.hashCode() * 163 + lastName.hashCode();
	}
	
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return firstName.equals(tp.firstName) && lastName.equals(tp.lastName);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return firstName + "\t" + lastName;
	}

	public int compareTo(TextPair arg0) {
		int cmp = firstName.compareTo(arg0.firstName);
	    if (cmp != 0) {
	      return cmp;
	    }
	    return lastName.compareTo(arg0.lastName);
	}

}
