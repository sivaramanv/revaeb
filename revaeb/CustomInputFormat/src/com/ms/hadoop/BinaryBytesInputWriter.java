package com.ms.hadoop;

import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.InputWriter;

/**
 * InputWriter that writes unmodified, unaugmented input bytes as key, and no
 * value.
 */
public class BinaryBytesInputWriter extends
    InputWriter<BinaryBytesWritable, NullWritable> {

  private static Logger logger =Logger.getLogger("MyLogger");
  private DataOutput output;

  @Override
  public void initialize(PipeMapRed pipeMapRed) throws IOException {
    super.initialize(pipeMapRed);
    output = pipeMapRed.getClientOutput();
  }

  @Override
  public void writeKey(BinaryBytesWritable key) throws IOException {
    output.write(key.getBytes(), 0, key.getLength());
  }

  @Override
  public void writeValue(NullWritable value) throws IOException {
    // No-op
  }
}
