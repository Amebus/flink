package org.apache.flink.api.bridge;

import org.apache.flink.api.serialization.StreamWriter;
import org.apache.flink.api.serialization.StreamWriterResult;
import org.apache.flink.api.tuple.IOclTuple;

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
						 OutputTupleInfo pOutputTupleInfo,
						 int pInputTuplesCount)
	{
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pInputTuplesCount)
				.writeStream();
		
		return super.OclReduce(pUserFunctionName,
							   vWriterResult.getStream(),
							   vWriterResult.getPositions(),
							   pOutputTupleDimension,
							   pOutputTupleInfo.toJniCompatibleFormat());
	}
}
