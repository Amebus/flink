package org.apache.flink.streaming.api.ocl.bridge;

import org.apache.flink.streaming.api.ocl.serialization.StreamWriter;
import org.apache.flink.streaming.api.ocl.serialization.StreamWriterResult;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;

public class OclBridge extends AbstractOclBridge
{
	private StreamWriter mStreamWriter;
	
	
	public OclBridge(StreamWriter pStreamWriter)
	{
		super("OclBridge");
		mStreamWriter = pStreamWriter;
	}
	
	public void initialize(String pKernelsFolders) { super.Initialize(pKernelsFolders); }
	public void dispose() { super.Dispose(); }
	
	public void listDevices()
	{
		ListDevices();
	}
	
	public boolean[] filter(String pUserFunctionName, Iterable< ? extends IOclTuple> pTuples, int pTuplesCount)
	{
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pTuplesCount)
				.writeStream();
		
		return super.OclFilter(pUserFunctionName, vWriterResult.getStream(), vWriterResult.getPositions());
	}
	
	public byte[] map(String pUserFunctionName,
					  Iterable< ? extends IOclTuple> pTuples,
					  int pOutputTupleDimension,
					  OutputTupleInfo pOutputTupleInfo,
					  int pInputTuplesCount)
	{
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pInputTuplesCount)
				.writeStream();
		
		return super.OclMap(pUserFunctionName,
							vWriterResult.getStream(),
							vWriterResult.getPositions(),
							pOutputTupleDimension,
							pOutputTupleInfo.toJniCompatibleFormat());
	}
	
	public byte[] reduce(String pUserFunctionName,
						 Iterable< ? extends IOclTuple> pTuples,
						 int pOutputTupleDimension,
						 byte[] pIdentity,
						 int pInputTuplesCount,
						 int pWorkGroupSize)
	{
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pInputTuplesCount)
				.writeStream();
		
		
		System.out.println("Identity: ");
		for(byte vByte : pIdentity)
		{
			System.out.print(" ");
			System.out.print(vByte);
			System.out.print(" ");
		}
		
		System.out.println();
		
		return super.OclReduce(pUserFunctionName,
							   vWriterResult.getStream(),
							   vWriterResult.getPositions(),
							   pOutputTupleDimension,
							   pIdentity,
							   pWorkGroupSize);
	}
}
