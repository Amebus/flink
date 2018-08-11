package org.apache.flink.api.bridge;

import org.apache.flink.api.bridge.AbstractOclBridge;
import org.apache.flink.api.serialization.StreamWriter;
import org.apache.flink.api.serialization.StreamWriterResult;
import org.apache.flink.api.tuple.IOclTuple;

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
	
	public boolean[] filter(String pUserFunctionName, List< ? extends IOclTuple> pTuples)
	{
		StreamWriterResult vWriterResult =
			StreamWriter.getStreamWriter().setTupleList(pTuples).writeStream();
		
		return super.OclFilter(pUserFunctionName, vWriterResult.getStream(), vWriterResult.getPositions());
	}
	
	public byte[] map(String pUserFunctionName, List< ? extends IOclTuple> pTuples)
	{
		StreamWriterResult vWriterResult =
			StreamWriter.getStreamWriter().setTupleList(pTuples).writeStream();
		
		return super.OclMap(pUserFunctionName, vWriterResult.getStream(), vWriterResult.getPositions());
	}
}
