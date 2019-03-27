package org.apache.flink.api.serialization.reader;

import org.apache.flink.api.engine.builder.options.DefaultsValues;
import org.apache.flink.api.serialization.StreamReader;

public abstract class StreamIterator implements IStreamReaderIterator
{
	public static final String DESERIALIZATION_ERROR = "Object type not recognized, unable to deserialize it";
	public static final char STRING_END = '\u0000';
	
	protected int mArity;
	protected byte[] mStream;
	protected Object[] mResult;
	protected int mIndex;
	protected int mTypeIndex;
	protected int mResultIndex;
	protected int mStringLength;
	
	
	protected StreamIterator(StreamReader pStreamReader)
	{
		byte vArity = pStreamReader.getArity();
		mStream = pStreamReader.getStream();
		if (mArity != vArity)
		{
			mArity = vArity;
			mResult = new Object[vArity];
		}
		mIndex = 1 + mArity;
		mTypeIndex = 1;
		mResultIndex = 0;
		mStringLength = 0;
	}
	
	protected Object[] readValuesFromStream()
	{
		byte vType;
		
		for (mResultIndex = 0; mResultIndex < mArity; mResultIndex++)
		{
			vType = mStream[mTypeIndex++];
			switch (vType)
			{
				case DefaultsValues.DefaultsSerializationTypes.INT:
					integerFromByteArray();
					break;
				case DefaultsValues.DefaultsSerializationTypes.DOUBLE:
					doubleFromByteArray();
					break;
				case DefaultsValues.DefaultsSerializationTypes.STRING:
					stringFromByteArray();
					break;
				default:
					throw new IllegalArgumentException(DESERIALIZATION_ERROR);
			}
		}
		mTypeIndex = 1;
		return mResult;
	}
	
	protected abstract void integerFromByteArray();
	
	protected abstract void doubleFromByteArray();
	
	protected abstract void stringLengthFromByteArray();
	
	protected void stringFromByteArray()
	{
		stringLengthFromByteArray();
		int vStringLength = mStringLength;
		int vIndex = mIndex + mStringLength - 1;
		while (mStream[vIndex] == STRING_END && vStringLength >= 0)
		{
			vIndex--;
			vStringLength--;
		}
		mResult[mResultIndex] = new String(mStream, mIndex, vStringLength);
		mIndex+=mStringLength;
	}
	
	@Override
	public boolean hasNext()
	{
		return mIndex < mStream.length;
	}
}
