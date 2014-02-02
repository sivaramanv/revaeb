package com.ms.hadoop;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 * @author Prateek Mathur
 * 
 */
public class BinaryFixedLengthRecordReader implements
		RecordReader<BinaryBytesWritable, NullWritable> {

	private long recordLength = 0;
	private long splitStart;

	// the end point in our split
	private long splitEnd;

	// our current position in the split
	// /private long currentPosition;
	private boolean isProcessed = false;

	// the length of a record
	// private int recordLength;

	// reference to the input stream
	private FSDataInputStream fileInputStream;

	// the input byte counter
	// private Counter inputByteCounter;

	// reference to our FileSplit
	private FileSplit fileSplit;

	// referent to the Job conf
	// private Configuration conf;

	private int position;

	private static Logger logger = Logger.getLogger("MyLogger2");

	// the record value
	// private BytesWritable recordValue = null;
	public BinaryFixedLengthRecordReader(InputSplit split, Configuration conf,
			long recordLength) {
		try {
			this.recordLength = Integer
					.parseInt(conf
							.get(BinaryFixedLengthInputFormat.BINARY_INPUT_FORMAT_LENGTH));
			;
			this.fileSplit = (FileSplit) split;
			// this.conf = conf;

			splitStart = fileSplit.getStart();
			splitEnd = splitStart + fileSplit.getLength();
			Path file = fileSplit.getPath();
			// get the filesystem
			FileSystem fs;

			fs = file.getFileSystem(conf);
			this.fileInputStream = fs.open(file);
			fileInputStream.seek(splitStart);
			logger.info("Split from RecordReader method : " + split.getLength()
					+ "Split start:" + splitStart + "Split End:" + splitEnd);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * A workaround to get through the stdin without consuming "\n" or "\r"
	 * characters which may be part of data.
	 * 
	 * @param rawBytes
	 * @return
	 */
	private byte[] transformRawBytesToHex(byte[] rawBytes) {
		StringBuilder sb = new StringBuilder();
		String lineseparator = System.getProperty("line.separator");
		int recordCounter = 0;
		byte[] byteRecords = new byte[(int) recordLength];
		for (int i = 0; i < rawBytes.length; i++) {
			// sb.append(String.format("%02X", b));
			// logger.info("Byte records initialized and length:"+byteRecords.length);
			byteRecords[recordCounter] = rawBytes[i];
			recordCounter++;
			if (recordCounter == recordLength) {
				String s = bytesToHex(byteRecords);
				// logger.info("***hexastring" + s);
				sb.append(s);
				sb.append(lineseparator);
				// sb=new StringBuilder();
				recordCounter = 0;
				byteRecords = new byte[(int) recordLength];
			}

		}
		return sb.toString().getBytes();
	}

	@Override
	public boolean next(BinaryBytesWritable key, NullWritable value)
			throws IOException {
		// TODO Auto-generated method stub
		long length = this.fileSplit.getLength();
		byte[] rawBytes = new byte[(int) length];
		int read = this.fileInputStream.read(rawBytes, 0, (int) length);
		logger.info("raw bytes length***" + rawBytes.length);
		byte[] hexBytes = transformRawBytesToHex(rawBytes);
		key.set(hexBytes);
		key.setLength((int) hexBytes.length);// = new BytesWritable().
		logger.info("Split from next method : " + value.toString());
		isProcessed = true;
		position = (int) splitEnd;
		return read >= 0;
	}

	@Override
	public BinaryBytesWritable createKey() {
		// TODO Auto-generated method stub
		return new BinaryBytesWritable();
	}

	@Override
	public NullWritable createValue() {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return position;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (fileInputStream != null) {
			fileInputStream.close();
		}
	}

	@Override
	public float getProgress() throws IOException {
		if (isProcessed) {
			return 1.0f;
		} else {
			return 0.0f;
		}
	}

	public String bytesToHex(byte[] bytes) {
		final char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
				'9', 'A', 'B', 'C', 'D', 'E', 'F' };
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

}
