package org.apache.flink.api.serialization;


import org.apache.flink.api.engine.builder.options.DefaultsValues;
import org.apache.flink.api.tuple.IOclTuple;

public class StreamWriter
{
	private Iterable<? extends IOclTuple> mTupleList;
	private int mTupleListSize = -1;
	
	private byte[] mVarTypes;
	private int mIndex;
	
	private int mTempInteger;
	private Double mTempDouble;
	private String mTempString;
	
	private static StreamWriter sStreamWriter = new StreamWriter();
	
	public static StreamWriter getStreamWriter()
	{
		return getStreamWriter(false);
	}
	
	public static StreamWriter getStreamWriter(boolean forParallelComputation)
	{
		if(forParallelComputation)
			return new StreamWriter();
		else
			return sStreamWriter;
	}
	
	private StreamWriter()
	{
	
	}
	
	public StreamWriter setTupleList(Iterable<? extends IOclTuple> pTupleList)
	{
		mTupleList = pTupleList;
		return this;
	}
	
	public StreamWriter setTupleListSize(int pTupleListSize)
	{
		mTupleListSize = pTupleListSize;
		return this;
	}
	
	public StreamWriterResult writeStream()
	{
		if (mTupleList == null || mTupleListSize < 1)
		{
			// throw new IllegalArgumentException("The list cannot be empty");
			return new StreamWriterResult(new byte[0], new int[0]);
		}
		
		IOclTuple vTemplateTuple = mTupleList.iterator().next();
		byte vArity = vTemplateTuple.getArityOcl();
		int vStreamLength = 1 + vArity;
		mVarTypes = getTypes(vTemplateTuple);
		mIndex = vStreamLength;
		
		for (IOclTuple vTuple : mTupleList)
		{
			vStreamLength += getBytesDim(vTuple);
		}
		
		final byte[] vStream = new byte[vStreamLength];
		final int[] vTupleIndexes = new int[mTupleListSize];
		
		vStream[0] = vArity;
		
		System.arraycopy(mVarTypes, 0, vStream, 1, mVarTypes.length);
		
		int vI = 0;
		for (IOclTuple vTuple : mTupleList)
		{
			vTupleIndexes[vI++] = writeStream(vTuple, vStream);
		}
		
		return new StreamWriterResult(vStream, vTupleIndexes);
	}
	
	
	private byte[] getTypes(IOclTuple pTuple)
	{
		byte[] vResult = new byte[pTuple.getArityOcl()];
		int vI = 0;
		Object vT;
		
		for (int i = 0; i<vResult.length; i++)
		{
			vT = pTuple.getFieldOcl(i);
			switch (vT.getClass().getName())
			{
				case "java.lang.Integer":
					vResult[vI++] = DefaultsValues.DefaultsSerializationTypes.INT;
					break;
				case "java.lang.Double":
					vResult[vI++] = DefaultsValues.DefaultsSerializationTypes.DOUBLE;
					break;
				case "java.lang.String":
					vResult[vI++] = DefaultsValues.DefaultsSerializationTypes.STRING;
					break;
				default:
					throw new IllegalArgumentException("Object type not recognized, unable to serialize it");
			}
		}
		return vResult;
	}
	
	private int getBytesDim(IOclTuple pTuple)
	{
		byte vArity = pTuple.getArityOcl();
		int vDim = 0;
		int vIndex = 0;
		Object vT;
		
		for (int i = 0; i < vArity; i++)
		{
			vT = pTuple.getFieldOcl(i);
			switch (mVarTypes[vIndex++])
			{
				case DefaultsValues.DefaultsSerializationTypes.DOUBLE:
					vDim += Dimensions.DOUBLE;
					break;
				case DefaultsValues.DefaultsSerializationTypes.STRING:
					vDim += (((String)vT).length() + 1);
				case DefaultsValues.DefaultsSerializationTypes.INT:
					vDim += Dimensions.INT;
			}
		}
		
		return vDim;
	}
	
	private int writeStream(IOclTuple pTuple, byte[] pStream)
	{
		byte vArity = pStream[0];
		int vStartIndex = mIndex;
		
		for (int i = 0; i < vArity; i++)
		{
			switch (mVarTypes[i])
			{
				case DefaultsValues.DefaultsSerializationTypes.INT:
					mTempInteger = pTuple.getField(i);
					insertInt(pStream);
					break;
				case DefaultsValues.DefaultsSerializationTypes.DOUBLE:
					mTempDouble = pTuple.getField(i);
					insertDouble(pStream);
					break;
				case DefaultsValues.DefaultsSerializationTypes.STRING:
					mTempString = pTuple.getField(i);
					insertString(pStream);
					break;
			}
		}
		
		return vStartIndex;
	}
	
	private void insertInt(byte[] pStream)
	{
		pStream[mIndex++] = (byte)(mTempInteger >> 24);
		pStream[mIndex++] = (byte)(mTempInteger >> 16);
		pStream[mIndex++] = (byte)(mTempInteger >> 8);
		pStream[mIndex++] = (byte) mTempInteger;
	}
	
	private void insertDouble(byte[] pStream)
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
	
	private void insertString(byte[] pStream)
	{
		byte[] vStream = mTempString.getBytes();
		insertStringLength(pStream, vStream.length + 1);
		for (int i = 0; i < vStream.length && mIndex < pStream.length; i++, mIndex++)
		{
			pStream[mIndex] = vStream[i];
		}
		pStream[mIndex++] = '\u0000';
		// int vLength = pValue.length();
		// insertStringLength(pStream, vLength);
		// for (int i = 0; i < vLength && mIndex < pStream.length; i++, mIndex++)
		// {
		// 	pStream[mIndex] = (byte) pValue.charAt(i);
		// }
	}
	
	private void insertStringLength(byte[] pStream, int pValue)
	{
		pStream[mIndex++] = (byte)(pValue >> 24);
		pStream[mIndex++] = (byte)(pValue >> 16);
		pStream[mIndex++] = (byte)(pValue >> 8);
		pStream[mIndex++] = (byte)pValue;
	}
}
