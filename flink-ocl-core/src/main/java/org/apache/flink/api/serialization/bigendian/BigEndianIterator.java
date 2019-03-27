package org.apache.flink.api.serialization.bigendian;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.reader.StreamIterator;

public abstract class BigEndianIterator extends StreamIterator
{
	protected BigEndianIterator(StreamReader pStreamReader)
	{
		super(pStreamReader);
	}
	
	protected void integerFromByteArray() {
		mResult[mResultIndex] = (mStream[mIndex++]       ) << 24 |
								(mStream[mIndex++] & 0xFF) << 16 |
								(mStream[mIndex++] & 0xFF) << 8  |
								(mStream[mIndex++] & 0xFF);
	}
	
	protected void doubleFromByteArray() {
		mResult[mResultIndex] = Double
			.longBitsToDouble(((long)mStream[mIndex++]		 ) << 56 |
							  ((long)mStream[mIndex++] & 0xFF) << 48 |
							  ((long)mStream[mIndex++] & 0xFF) << 40 |
							  ((long)mStream[mIndex++] & 0xFF) << 32 |
							  ((long)mStream[mIndex++] & 0xFF) << 24 |
							  ((long)mStream[mIndex++] & 0xFF) << 16 |
							  ((long)mStream[mIndex++] & 0xFF) << 8  |
							  ((long)mStream[mIndex++] & 0xFF));
	}
	
	protected void stringLengthFromByteArray()
	{
		mStringLength = (mStream[mIndex++]       ) << 24 |
						(mStream[mIndex++] & 0xFF) << 16 |
						(mStream[mIndex++] & 0xFF) << 8  |
						(mStream[mIndex++] & 0xFF);
	}
}
