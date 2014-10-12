package edu.rosehulman.postcn;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class RatioCalc extends EvalFunc<Float>{

	@Override
	public Float exec(Tuple arg0) throws IOException {
		if (arg0 == null || arg0.size() < 2) {
			return null;
		}
		
		long hits = (Long) arg0.get(0);
		long total = (Long) arg0.get(1);
		
		return total > hits ? (float)hits/total : 0;
	}

}
