package edu.rosehulman.postcn;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Concat extends EvalFunc<String> {

	@Override
	public String exec(Tuple arg0) throws IOException {
		if (arg0 == null || arg0.size() < 2) {
			return null;
		}
		StringBuilder builder = new StringBuilder();
		builder.append((String)arg0.get(0));
		builder.append(" ");
		builder.append((String)arg0.get(1));
		
		return builder.toString();
	}

}
