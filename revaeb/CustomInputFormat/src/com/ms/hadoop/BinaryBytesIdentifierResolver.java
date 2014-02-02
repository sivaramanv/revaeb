package com.ms.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.streaming.io.IdentifierResolver;

public class BinaryBytesIdentifierResolver extends IdentifierResolver {

	public static final String TYPED_BYTES_ID = "binarybytes";
	/**
	 * Resolves a given identifier. This method has to be called before calling
	 * any of the getters.
	 */
	@Override
	public void resolve(String identifier) {
		if (identifier.equalsIgnoreCase(TYPED_BYTES_ID)) {
			setInputWriterClass(BinaryBytesInputWriter.class);
			setOutputReaderClass(BinaryBytesOutputReader.class);
			setOutputKeyClass(BinaryBytesWritable.class);
			setOutputValueClass(NullWritable.class);
		} else { // assume TEXT_ID
			super.resolve(identifier);
		}
	}
}
