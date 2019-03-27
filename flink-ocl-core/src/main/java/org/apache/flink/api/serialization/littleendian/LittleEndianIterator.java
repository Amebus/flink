package org.apache.flink.api.serialization.littleendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.reader.StreamIterator;

public abstract class LittleEndianIterator extends StreamIterator
{
	protected int mEndianIndex;
	
	protected LittleEndianIterator(StreamReader pStreamReader)
	{
		super(pStreamReader);
	}
	
	protected void integerFromByteArray() {
		mIndex+=4;
		mEndianIndex = mIndex;
		mResult[mResultIndex] = (mStream[mEndianIndex--]       ) << 24 |
								(mStream[mEndianIndex--] & 0xFF) << 16 |
								(mStream[mEndianIndex--] & 0xFF) << 8  |
								(mStream[mEndianIndex--] & 0xFF);
	}
	
	protected void doubleFromByteArray() {
		mIndex+=4;
		mEndianIndex = mIndex;
		mResult[mResultIndex] = Double
			.longBitsToDouble(((long)mStream[mEndianIndex--]		 ) << 56 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 48 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 40 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 32 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 24 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 16 |
							  ((long)mStream[mEndianIndex--] & 0xFF) << 8  |
							  ((long)mStream[mEndianIndex--] & 0xFF));
	}
	
	protected void stringLengthFromByteArray()
	{
		mIndex+=4;
		mEndianIndex = mIndex;
		mStringLength = (mStream[mEndianIndex--]       ) << 24 |
						(mStream[mEndianIndex--] & 0xFF) << 16 |
						(mStream[mEndianIndex--] & 0xFF) << 8  |
						(mStream[mEndianIndex--] & 0xFF);
	}
}
