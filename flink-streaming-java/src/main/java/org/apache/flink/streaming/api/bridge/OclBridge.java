package org.apache.flink.streaming.api.bridge;

import org.apache.flink.streaming.api.serialization.StreamWriter;
import org.apache.flink.streaming.api.serialization.StreamWriterResult;
import org.apache.flink.streaming.api.tuple.IOclTuple;

import java.util.List;

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
					  List< ? extends IOclTuple> pTuples,
					  int pOutputTupleDimension,
					  OutputTupleInfo pOutputTupleInfo)
	{
		StreamWriterResult vWriterResult =
			StreamWriter.getStreamWriter()
						.setTupleList(pTuples)
						.setTupleListSize(pTuples.size())
						.writeStream();
		
		return super.OclMap(pUserFunctionName,
							vWriterResult.getStream(),
							vWriterResult.getPositions(),
							pOutputTupleDimension,
							pOutputTupleInfo.toJniCompatibleFormat());
	}
}
