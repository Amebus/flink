package org.apache.flink.api.bridge;

import org.apache.flink.api.serialization.StreamWriter;
import org.apache.flink.api.serialization.StreamWriterResult;
import org.apache.flink.api.tuple.IOclTuple;

public class OclBridge extends AbstractOclBridge
{
	
	public OclBridge()
	{
		super("OclBridge");
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
			StreamWriter.getStreamWriter()
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
			StreamWriter.getStreamWriter()
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
			StreamWriter.getStreamWriter()
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
