package org.apache.flink.api.serialization;

import com.sun.istack.internal.NotNull;
import org.apache.flink.api.serialization.reader.IStreamReaderIterator;
import org.apache.flink.api.serialization.reader.StreamIterator;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.api.tuple.Tuple3Ocl;

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
	public @NotNull Iterator<IOclTuple> iterator()
	{
		return streamReaderIterator();
	}
	
	public abstract IStreamReaderIterator streamReaderIterator();
}
