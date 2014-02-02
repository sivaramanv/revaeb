package com.ms.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * An OutputFormat that writes unmodified, unaugmented bytes as key and no
 * value.
 * </p>
 * 
 * <p>
 * Output is never compressed.
 * </p>
 */
public class BinaryBytesOutputFormat extends
		FileOutputFormat<BinaryBytesWritable, NullWritable> {
	private static final Log LOG = LogFactory
			.getLog(BinaryBytesOutputFormat.class);

	/**
	 * RecordWriter that writes raw bytes as key and no value.
	 */
	public static class JustBytesRecordWriter implements
			RecordWriter<BinaryBytesWritable, NullWritable> {
		private final FSDataOutputStream outputStream;

		public JustBytesRecordWriter(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			outputStream.close();
		}

		@Override
		public void write(BinaryBytesWritable bytes, NullWritable none)
				throws IOException {
			outputStream.write(bytes.getBytes(), 0, bytes.getLength());
		}
	}

	/**
	 * @return {@link JustBytesRecordWriter}
	 */
	@Override
	public RecordWriter<BinaryBytesWritable, NullWritable> getRecordWriter(
			FileSystem fileSystem, JobConf conf, String name,
			Progressable progress) throws IOException {
		Path file = FileOutputFormat.getTaskOutputPath(conf, name);
		LOG.info("New JustBytesRecordWriter for file " + file);
		return new JustBytesRecordWriter(file.getFileSystem(conf).create(file,
				progress));
	}
}
