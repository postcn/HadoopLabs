package edu.rosehulman.postcn.Lab2SequenceFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceFileWriter {

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.out.println("Usage: SequenceFileWriter <local dir> <hdfs output>");
			System.exit(1);
		}
		String localDir = args[0];
		String remoteOutput = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(remoteOutput), conf);
		Path path = new Path(remoteOutput);
		
		Text key = new Text();
		Text value = new Text();
		
		File local = new File(localDir);
		@SuppressWarnings("deprecation")
		Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
		for (File f: local.listFiles()) {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			String line;
			while((line = reader.readLine()) != null) {
				key.set(f.getName());
				value.set(line);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
			reader.close();
		}
		IOUtils.closeStream(writer);
	}
}
