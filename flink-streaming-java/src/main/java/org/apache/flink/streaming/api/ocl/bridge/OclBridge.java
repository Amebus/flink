package org.apache.flink.streaming.api.ocl.bridge;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.ocl.serialization.StreamWriter;
import org.apache.flink.streaming.api.ocl.serialization.StreamWriterResult;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;

public class OclBridge extends AbstractOclBridge
{
	private StreamWriter mStreamWriter;
	private StopWatch mSerialisationStopWatch;
	private long mSerializationTime;
	
	
	public OclBridge(StreamWriter pStreamWriter)
	{
		super("OclBridge");
		mStreamWriter = pStreamWriter;
		mSerializationTime = 0;
		mSerialisationStopWatch = new StopWatch();
	}
	
	public void initialize(String pKernelsFolders) { super.Initialize(pKernelsFolders); }
	public void dispose() { super.Dispose(); }
	
	public void listDevices()
	{
		ListDevices();
	}
	
	public long[] gGetKernelProfiling(String pUserFunctionName)
	{
		return this.GetKernelProfiling(pUserFunctionName);
	}
	
	public Tuple2<boolean[], Long> filter(String pUserFunctionName, Iterable< ? extends IOclTuple> pTuples, int pTuplesCount)
	{
		mSerialisationStopWatch.start();
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pTuplesCount)
				.writeStream();
		mSerializationTime = mSerialisationStopWatch.getNanoTime();
		mSerialisationStopWatch.reset();
		
		return new Tuple2<>(
			super.OclFilter(pUserFunctionName, vWriterResult.getStream(), vWriterResult.getPositions()),
			mSerializationTime);
	}
	
	public Tuple2<byte[], Long> map(String pUserFunctionName,
					  Iterable< ? extends IOclTuple> pTuples,
					  int pOutputTupleDimension,
					  OutputTupleInfo pOutputTupleInfo,
					  int pInputTuplesCount)
	{
		mSerialisationStopWatch.start();
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pInputTuplesCount)
				.writeStream();
		mSerializationTime = mSerialisationStopWatch.getNanoTime();
		mSerialisationStopWatch.reset();
		
		return new Tuple2<>(
			super.OclMap(pUserFunctionName,
						 vWriterResult.getStream(),
						 vWriterResult.getPositions(),
						 pOutputTupleDimension,
						 pOutputTupleInfo.toJniCompatibleFormat()),
			mSerializationTime);
	}
	
	public Tuple2<byte[], Long> reduce(String pUserFunctionName,
						 Iterable< ? extends IOclTuple> pTuples,
						 int pOutputTupleDimension,
						 byte[] pIdentity,
						 int pInputTuplesCount,
						 int pWorkGroupSize)
	{
		mSerialisationStopWatch.start();
		StreamWriterResult vWriterResult =
			mStreamWriter
				.setTupleList(pTuples)
				.setTupleListSize(pInputTuplesCount)
				.writeStream();
		mSerializationTime = mSerialisationStopWatch.getNanoTime();
		mSerialisationStopWatch.reset();
		
		System.out.println("Identity: ");
		for(byte vByte : pIdentity)
		{
			System.out.print(" ");
			System.out.print(vByte);
			System.out.print(" ");
		}
		
		System.out.println();
		
		byte[] vResult = super.OclReduce(pUserFunctionName,
										vWriterResult.getStream(),
										vWriterResult.getPositions(),
										pOutputTupleDimension,
										pIdentity,
										pWorkGroupSize);
		
		return new Tuple2<>(vResult, mSerializationTime);
	}
}
