package edu.rosehulman.postcn.Lab3CustomInputFormat;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FileSearchRecordReader extends RecordReader<Text, IntWritable> {
	private FileSplit fileSplit;
	private Configuration conf;
	private IntWritable count;
	private boolean processed = false;
	private String searchValue;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
		searchValue = context.getConfiguration().get(CustomWordCountRunner.SEARCH_KEY);
	}
	
	public void setSearchValue(String value) {
		this.searchValue = value;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				String fullContents = new String(contents);
				int count = StringUtils.countMatches(fullContents, searchValue);
				this.count = new IntWritable(count);
			}
			finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		Path file = fileSplit.getPath();
		return new Text(file.getName());
	}

	@Override
	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		return count;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void close() throws IOException {
		//Do nothing.
	}

}
