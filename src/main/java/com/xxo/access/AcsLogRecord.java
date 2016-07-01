package com.xxo.access;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class AcsLogRecord implements WritableComparable<Object>{

	public String duration;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		duration = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(duration);
	}

	@Override
	public int compareTo(Object o) {
		return 0;
	}

}
