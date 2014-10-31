package edu.rosehulman.postcn;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ScoreConvert extends EvalFunc<String> {

	@Override
	public String exec(Tuple arg0) throws IOException {
		if (arg0 == null || arg0.size() < 1) {
			return null;
		}
		
		double grade = (Double) arg0.get(0);
		
		if (grade <= 60) {
			return "F";
		}
		else if (grade <= 70) {
			return "D";
		}
		else if (grade <=80) {
			return "C";
		}
		else if (grade <=90) {
			return "B";
		}
		else {
			return "A";
		}
	}

}
