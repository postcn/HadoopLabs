package edu.rosehulman.postcn.Lab2SequenceFile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReader {

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.out.println("Usage: SequenceFileReader <remote file>");
			System.exit(1);
		}
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		
		@SuppressWarnings("deprecation")
		Reader reader = new Reader(fs, path, conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long position = reader.getPosition();
		while(reader.next(key, value)) {
			String syncSeen = reader.syncSeen() ? "*" : "";
			System.out.printf("[%s%S]\t%s\t%s\n", position, syncSeen, key, value);
			position = reader.getPosition();
		}
		
		IOUtils.closeStream(reader);
	}
}
