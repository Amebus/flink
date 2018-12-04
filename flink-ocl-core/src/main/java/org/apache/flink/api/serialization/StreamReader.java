package org.apache.flink.api.serialization;

import com.sun.istack.internal.NotNull;
import org.apache.flink.api.defaults.DefaultsSerializationTypes;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.api.tuple.Tuple3Ocl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class StreamReader implements Iterable<IOclTuple>
{
	public static final String DIMENSION_ERROR = "Tuple dimension not supported";
	public static final String DESERIALIZATION_ERROR = "Object type not recognized, unable to deserialize it";
	public static final char STRING_END = '\u0000';
	
	private byte mArity;
	private byte[] mStream;
	
	private static StreamReader sStreamReader = new StreamReader();
	
	public static StreamReader getStreamReader()
	{
		return sStreamReader;
	}
	
	private StreamReader()
	{
	}
	
	public StreamReader setStream(byte[] pStream)
	{
		mStream = pStream;
		mArity = mStream[0];
		return this;
	}
	
	public byte[] getStream()
	{
		return mStream;
	}
	
	public byte getArity()
	{
		return mArity;
	}
	
	@SuppressWarnings("unchecked")
	public <R extends IOclTuple> List<R> getTupleList()
	{
		List<R> vResult = new LinkedList<>();
		
		for (IOclTuple vTuple : this)
		{
			vResult.add((R)vTuple);
		}
		
		return vResult;
	}
	
	
	@Override
	public @NotNull Iterator<IOclTuple> iterator()
	{
		switch (getArity())
		{
			case 1:
				return new Tuple1Iterator(this);//mTuple1Iterator.setStreamReader(this);
			case 2:
				return new Tuple2Iterator(this);//mTuple2Iterator.setStreamReader(this);
			case 3:
				return new Tuple3Iterator(this);//mTuple3Iterator.setStreamReader(this);
			default:
				throw new IllegalArgumentException(DIMENSION_ERROR);
		}
	}
	
	public IStreamReaderIterator streamReaderIterator()
	{
		return (IStreamReaderIterator)iterator();
	}
	
	public interface IStreamReaderIterator extends Iterator<IOclTuple>
	{
		<R extends IOclTuple> R nextTuple();
	}
	
	private static abstract class StreamIterator implements IStreamReaderIterator
	{
		private int mArity;
		private byte[] mStream;
		private Object[] mResult;
		private int mIndex;
		private int mTypeIndex;
		private int mResultIndex;
		private int mStringLength;
		
		
		StreamIterator(StreamReader pStreamReader)
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
		
		Object[] readValuesFromStream()
		{
			byte vType;
			
			for (mResultIndex = 0; mResultIndex < mArity; mResultIndex++)
			{
				vType = mStream[mTypeIndex++];
				switch (vType)
				{
					case DefaultsSerializationTypes.INT:
						integerFromByteArray();
						break;
					case DefaultsSerializationTypes.DOUBLE:
						doubleFromByteArray();
						break;
					case DefaultsSerializationTypes.STRING:
						stringFromByteArray();
						break;
					default:
						throw new IllegalArgumentException(DESERIALIZATION_ERROR);
				}
			}
			mTypeIndex = 1;
			return mResult;
		}
		
		private void integerFromByteArray() {
			mResult[mResultIndex] = (mStream[mIndex++]       ) << 24 |
									(mStream[mIndex++] & 0xFF) << 16 |
									(mStream[mIndex++] & 0xFF) << 8  |
									(mStream[mIndex++] & 0xFF);
		}
		
		private void doubleFromByteArray() {
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
		
		private void stringLengthFromByteArray()
		{
			mStringLength = (mStream[mIndex++]       ) << 24 |
							(mStream[mIndex++] & 0xFF) << 16 |
							(mStream[mIndex++] & 0xFF) << 8  |
							(mStream[mIndex++] & 0xFF);
		}
		
		private void stringFromByteArray()
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
	
	private static class Tuple1Iterator extends StreamIterator
	{
		
		Tuple1Iterator(StreamReader pStreamReader)
		{
			super(pStreamReader);
		}
		
		@Override
		public IOclTuple next()
		{
			Tuple1Ocl vTuple = new Tuple1Ocl();
			Object[] vValues = readValuesFromStream();
			
			vTuple.setField(vValues[0], 0);
			
			return vTuple;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public <R extends IOclTuple> R nextTuple()
		{
			return (R) next();
		}
	}
	
	private static class Tuple2Iterator extends StreamIterator
	{
		
		Tuple2Iterator(StreamReader pStreamReader)
		{
			super(pStreamReader);
		}
		
		@Override
		public IOclTuple next()
		{
			Tuple2Ocl vTuple = new Tuple2Ocl();
			Object[] vValues = readValuesFromStream();
			
			vTuple.setField(vValues[0], 0);
			vTuple.setField(vValues[1], 1);
			
			return vTuple;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public <R extends IOclTuple> R nextTuple()
		{
			return (R) next();
		}
	}
	
	private static class Tuple3Iterator extends StreamIterator
	{
		
		Tuple3Iterator(StreamReader pStreamReader)
		{
			super(pStreamReader);
		}
		
		@Override
		public IOclTuple next()
		{
			Tuple3Ocl vTuple = new Tuple3Ocl();
			Object[] vValues = readValuesFromStream();
			
			vTuple.setField(vValues[0], 0);
			vTuple.setField(vValues[1], 1);
			vTuple.setField(vValues[2], 1);
			
			return vTuple;
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public <R extends IOclTuple> R nextTuple()
		{
			return (R)next();
		}
	}
}
