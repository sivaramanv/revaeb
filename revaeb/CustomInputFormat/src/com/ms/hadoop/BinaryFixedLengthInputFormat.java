package com.ms.hadoop;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * @author Prateek Mathur.
 * 
 */
public class BinaryFixedLengthInputFormat extends
		FileInputFormat<BinaryBytesWritable, NullWritable> {

	public static final String BINARY_INPUT_FORMAT_LENGTH = "binary.input.format.length";
	private long recordLength = 0;// 130;

	private static Logger logger = Logger.getLogger("MyLogger");

	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		return true;
	}

	@Override
	public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		recordLength = Integer.parseInt(arg0.get(BINARY_INPUT_FORMAT_LENGTH));
		return super.getSplits(arg0, arg1);
	}

	@Override
	protected long computeSplitSize(long goalSize, long minSize, long blockSize) {
		// TODO Auto-generated method stub
		long defaultSize = super.computeSplitSize(goalSize, minSize, blockSize);
		if (defaultSize < recordLength) {
			return recordLength;
		}
		long splitSize = ((long) (Math.floor((double) defaultSize
				/ (double) recordLength)))
				* recordLength;
		logger.warning("record_len : " + recordLength);
		logger.warning("defaultSize : " + defaultSize);
		logger.warning("splitSize : " + splitSize);

		return splitSize;

	}

	@Override
	public RecordReader<BinaryBytesWritable, NullWritable> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		logger.warning("split : " + split.toString());
		return new BinaryFixedLengthRecordReader(split, job, recordLength);
	}

}
