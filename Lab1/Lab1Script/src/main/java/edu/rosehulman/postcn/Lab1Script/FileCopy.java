package edu.rosehulman.postcn.Lab1Script;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
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
		
		File localUpload = new File(args[0]);
		String hdfs = args[1];
		File localDownload = new File(args[2]);
		
		for (File file : localUpload.listFiles()) {
			InputStream in = new BufferedInputStream(new FileInputStream(file));
			
			Configuration conf = new Configuration();
			String location = hdfs+"/"+file.getName();
			FileSystem fs = FileSystem.get(URI.create(location), conf);
			System.out.print("Copying " + file.getName()+" to Hadoop: ");
			OutputStream out = fs.create(new Path(location), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
			System.out.println();
			System.out.println("Copying back to local system");
			OutputStream localOut = new BufferedOutputStream(new FileOutputStream(localDownload.getAbsolutePath()+"/"+file.getName()));
			IOUtils.copyBytes(fs.open(new Path(location)), localOut, 4096, true);
		}
		
		System.out.println("Operations complete");
		System.exit(0);
	}

}
