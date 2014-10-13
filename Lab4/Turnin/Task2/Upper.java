package edu.rosehulman.postcn;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Upper extends EvalFunc<String> {

	@Override
	public String exec(Tuple arg0) throws IOException {
		if (arg0 == null || arg0.size() < 1) {
			return null;
		}
		
		String val = (String) arg0.get(0);
		return val.toUpperCase();
	}
}
