package org.apache.flink.streaming.api.ocl.serialization.bigendian;

import org.apache.flink.streaming.api.ocl.serialization.StreamWriter;

public class BigEndianStreamWriter extends StreamWriter
{
	protected void insertInt(byte[] pStream)
	{
		pStream[mIndex++] = (byte)(mTempInteger >> 24);
		pStream[mIndex++] = (byte)(mTempInteger >> 16);
		pStream[mIndex++] = (byte)(mTempInteger >> 8);
		pStream[mIndex++] = (byte) mTempInteger;
	}
	
	protected void insertDouble(byte[] pStream)
	{
		long vL = Double.doubleToLongBits(mTempDouble);
		pStream[mIndex++] = (byte)((vL >> 56) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 48) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 40) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 32) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 24) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 16) & 0xFF);
		pStream[mIndex++] = (byte)((vL >> 8) & 0xFF);
		pStream[mIndex++] = (byte)(vL & 0xFF);
	}
	
	protected void insertStringLength(byte[] pStream, int pValue)
	{
		pStream[mIndex++] = (byte)(pValue >> 24);
		pStream[mIndex++] = (byte)(pValue >> 16);
		pStream[mIndex++] = (byte)(pValue >> 8);
		pStream[mIndex++] = (byte)pValue;
	}
}
