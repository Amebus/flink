package org.apache.flink.api.serialization;

import org.apache.flink.api.tuple.Tuple2Ocl;

public class StreamWriterResult extends Tuple2Ocl<byte[], int[]>
{
	public StreamWriterResult(byte[] pBytes, int[] pInts)
	{
		super(pBytes, pInts);
	}
	
	public StreamWriterResult(StreamWriterResult pResult)
	{
		super(pResult.getStream(), pResult.getPositions());
	}
	
	
	public byte[] getStream()
	{
		return super.getField(0);
	}
	
	public int[] getPositions()
	{
		return super.getField(1);
	}
}
