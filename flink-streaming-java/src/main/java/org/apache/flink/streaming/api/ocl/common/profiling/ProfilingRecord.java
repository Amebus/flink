package org.apache.flink.streaming.api.ocl.common.profiling;

public class ProfilingRecord
{
	
	/*
	 *
	 * KernelName | *
	 * KernelType | *
	 * Total | *
	 * Serialization | *
	 * Deserialization *
	 *
	 *
	 *
	 * Java to C++ | c++
	 * Computation | c++
	 * */
	
	
	private String mKernelName;
	private String mKernelType;
	
	private long mTotal;
	private long mSerialization;
	private long mDeserialization;
	
	private long mJavaToC;
	private long mKernelComputation;
	
	
	
	public ProfilingRecord(String pKernelName, String pKernelType)
	{
		mKernelName = pKernelName;
		mKernelType = pKernelType;
	}
	
	public String getKernelName()
	{
		return mKernelName;
	}
	public String getKernelType()
	{
		return mKernelType;
	}
	
	public long getTotal()
	{
		return mTotal;
	}
	public void setTotal(long pTotal)
	{
		mTotal = pTotal;
	}
	
	public long getSerialization()
	{
		return mSerialization;
	}
	public void setSerialization(long pSerialization)
	{
		mSerialization = pSerialization;
	}
	
	public long getDeserialization()
	{
		return mDeserialization;
	}
	public void setDeserialization(long pDeserialization)
	{
		mDeserialization = pDeserialization;
	}
	
	public long getJavaToC()
	{
		return mJavaToC;
	}
	public void setJavaToC(long pJavaToC)
	{
		mJavaToC = pJavaToC;
	}
	
	public long getKernelComputation()
	{
		return mKernelComputation;
	}
	public void setKernelComputation(long pKernelComputation)
	{
		mKernelComputation = pKernelComputation;
	}
}
