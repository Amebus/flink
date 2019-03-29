package org.apache.flink.api.serialization.littleendian;

import org.apache.flink.api.serialization.StreamWriter;

public class LittleEndianStreamWriter extends StreamWriter
{
	protected int mEndianIndex;
	
	protected void insertInt(byte[] pStream)
	{
		mIndex+=4;
		mEndianIndex = mIndex;
		pStream[--mEndianIndex] = (byte)(mTempInteger >> 24);
		pStream[--mEndianIndex] = (byte)(mTempInteger >> 16);
		pStream[--mEndianIndex] = (byte)(mTempInteger >> 8);
		pStream[--mEndianIndex] = (byte) mTempInteger;
	}
	
	protected void insertDouble(byte[] pStream)
	{
		mIndex+=8;
		mEndianIndex = mIndex;
		long vL = Double.doubleToLongBits(mTempDouble);
		pStream[--mEndianIndex] = (byte)((vL >> 56) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 48) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 40) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 32) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 24) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 16) & 0xFF);
		pStream[--mEndianIndex] = (byte)((vL >> 8) & 0xFF);
		pStream[--mEndianIndex] = (byte)(vL & 0xFF);
	}
	
	protected void insertStringLength(byte[] pStream, int pValue)
	{
		mIndex+=4;
		mEndianIndex = mIndex;
		pStream[--mEndianIndex] = (byte)(pValue >> 24);
		pStream[--mEndianIndex] = (byte)(pValue >> 16);
		pStream[--mEndianIndex] = (byte)(pValue >> 8);
		pStream[--mEndianIndex] = (byte)pValue;
	}
}
