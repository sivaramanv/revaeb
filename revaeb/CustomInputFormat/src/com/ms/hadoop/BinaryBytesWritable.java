package com.ms.hadoop;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

public class BinaryBytesWritable extends BinaryComparable implements
		WritableComparable<BinaryComparable> {
	private static final int DEFAULT_CAPACITY_BYTES = 64*1024;
	private byte[] bytes; // Actual data
	private int length; // Number of valid bytes in this.bytes

	/**
	 * Initialize a new JustBytesWritable containing no bytes, with the default
	 * capacity.
	 */
	public BinaryBytesWritable() {
		this(new byte[DEFAULT_CAPACITY_BYTES]);
		this.length = 0;
	}

	/**
	 * Initialize a new JustBytesWritable containing the given byte array
	 * directly (no copy is made).
	 * 
	 * The length is set to <tt>bytes.length</tt>.
	 */
	public BinaryBytesWritable(byte[] bytes) {
		super();
		set(bytes);
	}

	/**
	 * Make this object contain the given array directly (no copy is made).
	 * 
	 * The length is set to <tt>bytes.length</tt>.
	 */
	public void set(byte[] bytes) {
		if (bytes == null) {
			throw new IllegalArgumentException("Null array");
		}
		this.bytes = bytes;
		this.length = bytes.length;
	}

	/**
	 * Set the number of valid bytes in this object.
	 * 
	 * @param length
	 *            Cannot be negative or greater than <tt>getBytes().length</tt>.
	 */
	public void setLength(int length) {
		if (length < 0 || length > bytes.length) {
			throw new IllegalArgumentException("Invalid length: " + length);
		}
		this.length = length;
	}

	/**
	 * Read up to <tt>getBytes().length</tt> bytes from <tt>input</tt> and set
	 * the length to the number of bytes read.
	 * 
	 * Sets the length to zero (and does not modify any bytes) on and only on
	 * EOF.
	 */
	@Override
	public void readFields(DataInput input) throws IOException {
		/*
		 * We cheat and use the knowledge that streaming passes us a
		 * DataInputStream so we don't have to read byte by byte waiting for
		 * EOFException. This reduces CPU usage by half.
		 */
		length = Math.max(0, ((DataInputStream) input).read(bytes));
	}

	/**
	 * Write <tt>getLength()</tt> bytes to <tt>output</tt>.
	 */
	@Override
	public void write(DataOutput output) throws IOException {
		output.write(bytes, 0, length);
	}

	/**
	 * Return the array backing this object (no copy is made).
	 * 
	 * Note that only the first <tt>getLength()</tt> bytes are valid.
	 */
	@Override
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * Return the number of valid bytes in this object.
	 */
	@Override
	public int getLength() {
		return length;
	}
}
