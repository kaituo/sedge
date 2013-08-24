package edu.umass.cs.pig.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.apache.pig.data.DataByteArray;

public class DataByteArrayConverter {
	// private int toInt(DataByteArray d) {
	// byte[] db = d.get();
	// ByteBuffer bb = ByteBuffer.wrap(db);
	// return bb.getInt();
	// }

	public int toInt(DataByteArray d) {
		String bs = toString(d);
		return Integer.parseInt(bs);
	}

	// private DataByteArray toDataByteArray(int d) {
	// byte[] bytes = ByteBuffer.allocate(4).putInt(d).array();
	// DataByteArray bb = new DataByteArray(bytes);
	// return bb;
	// }

	public DataByteArray toDataByteArray(int d) {
		String ds = Integer.toString(d);
		return toDataByteArray(ds);
	}

	// private double toDouble(DataByteArray d) {
	// byte[] db = d.get();
	// ByteBuffer bb = ByteBuffer.wrap(db);
	// return bb.getDouble();
	// }

	public double toDouble(DataByteArray d) {
		String bs = toString(d);
		return Double.parseDouble(bs);
	}

	// private DataByteArray toDataByteArray(double d) {
	// byte[] bytes = ByteBuffer.allocate(8).putDouble(d).array();
	// DataByteArray bb = new DataByteArray(bytes);
	// return bb;
	// }

	public DataByteArray toDataByteArray(double d) {
		String ds = Double.toString(d);
		return toDataByteArray(ds);
	}

	// private float toFloat(DataByteArray d) {
	// byte[] db = d.get();
	// ByteBuffer bb = ByteBuffer.wrap(db);
	// return bb.getFloat();
	// }

	public float toFloat(DataByteArray d) {
		String bs = toString(d);
		return Float.parseFloat(bs);
	}

	public DataByteArray toDataByteArray(float d) {
		String ds = Float.toString(d);
		return toDataByteArray(ds);
	}

	// private DataByteArray toDataByteArray(float d) {
	// byte[] bytes = ByteBuffer.allocate(4).putFloat(d).array();
	// DataByteArray bb = new DataByteArray(bytes);
	// return bb;
	// }

	// private long toLong(DataByteArray d) {
	// byte[] db = d.get();
	// ByteBuffer bb = ByteBuffer.wrap(db);
	// return bb.getLong();
	// }

	public long toLong(DataByteArray d) {
		String bs = toString(d);
		return Long.parseLong(bs);
	}

	// private DataByteArray toDataByteArray(long d) {
	// byte[] bytes = ByteBuffer.allocate(4).putLong(d).array();
	// DataByteArray bb = new DataByteArray(bytes);
	// return bb;
	// }

	public DataByteArray toDataByteArray(long d) {
		String ds = Long.toString(d);
		return toDataByteArray(ds);
	}

	public String toString(DataByteArray d) {
		// Create the encoder and decoder for UTF-8
		Charset charset = Charset.forName("US-ASCII");
		byte[] db = d.get();
		ByteBuffer bb = ByteBuffer.wrap(db);
		CharsetDecoder decoder = charset.newDecoder();

		// Convert UTF-8 bytes in a ByteBuffer to a character ByteBuffer and
		// then to a string.
		// The new ByteBuffer is ready to be read.
		CharBuffer cbuf;
		String s = null;
		try {
			cbuf = decoder.decode(bb);
			s = cbuf.toString();

		} catch (CharacterCodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return s;
	}

	public DataByteArray toDataByteArray(String d) {
		// Create the encoder and decoder for UTF-8
		Charset charset = Charset.forName("US-ASCII");
		CharsetEncoder encoder = charset.newEncoder();
		// Convert a string to ISO-LATIN-1 bytes in a ByteBuffer
		// The new ByteBuffer is ready to be read.
		ByteBuffer bbuf;
		DataByteArray bb = null;
		try {
			bbuf = encoder.encode(CharBuffer.wrap(d));
			byte[] bytes = bbuf.array();
			bb = new DataByteArray(bytes);
		} catch (CharacterCodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return bb;
	}

}
