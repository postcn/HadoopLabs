package edu.rosehulman.postcn.Lab1Script;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopy {

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage FileCopy <local dir> <hdfs dir> <local dir>");
			System.exit(-1);
		}
		
		String localUpload = args[0];
		String hdfs = args[1];
		String localDownload = args[2];
		
		InputStream in = new BufferedInputStream(new FileInputStream(localUpload));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs), conf);
		System.out.print("Copying to Hadoop:");
		OutputStream out = fs.create(new Path(hdfs), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
		System.out.println();
		System.out.println("Copying back to local system");
		OutputStream localOut = new BufferedOutputStream(new FileOutputStream(localDownload));
		IOUtils.copyBytes(fs.open(new Path(hdfs)), localOut, 4096, true);
		System.out.println("Operations complete");
		System.exit(0);
	}

}
