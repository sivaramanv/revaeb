package com.ms.hadoop;


import java.io.DataInput;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.OutputReader;

/**
 * OutputReader that reads unmodified, unaugmented bytes from the client as key,
 * and no value.
 */
public class BinaryBytesOutputReader extends
		OutputReader<BinaryBytesWritable, NullWritable> {
  private DataInput input;
  private BinaryBytesWritable key;
  private static Logger logger =Logger.getLogger("MyLogger");

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    input = pipeMapRed.getClientInput();
    key = new BinaryBytesWritable();
  }

  @Override
  public boolean readKeyValue() throws IOException {
    key.readFields(input);
    logger.warning("****Prat output reader:" + new String(key.getBytes()));
    return key.getLength() > 0; // Should only be zero on EOF
  }

  @Override
  public BinaryBytesWritable getCurrentKey() throws IOException {
    return key;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException {
    return NullWritable.get();
  }

  @Override
  public String getLastOutput() {
    throw new UnsupportedOperationException(
        "Use new String(bytes, encoding) to make a String from bytes");
  }
}