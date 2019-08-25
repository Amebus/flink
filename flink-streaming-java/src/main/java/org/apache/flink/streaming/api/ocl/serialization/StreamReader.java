package org.apache.flink.streaming.api.ocl.serialization;

import org.apache.flink.streaming.api.ocl.serialization.reader.IStreamReaderIterator;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public abstract class StreamReader implements Iterable<IOclTuple>
{
	public static final String DIMENSION_ERROR = "Tuple dimension not supported";
	private byte mArity;
	private byte[] mStream;
	
	protected StreamReader()
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
	public Iterator<IOclTuple> iterator()
	{
		return streamReaderIterator();
	}
	
	public abstract IStreamReaderIterator streamReaderIterator();
}
