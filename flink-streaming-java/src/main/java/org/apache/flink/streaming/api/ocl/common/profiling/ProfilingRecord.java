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
	 * Java to C++ | c++
	 * Host to GPU ? | c++
	 * GPU to Host | c++
	 * */
	
	
	private String mKernelName;
	private String mKernelType;
	
	private long mTotal;
	private long mSerialization;
	private long mDeserialization;
	
	private long mJavaToC;
	private long mCToJava;
	
	private long mHostToGpu; // ?
	private long mGpuToHost; // ?
	
	
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
	
	public long getCToJava()
	{
		return mCToJava;
	}
	
	public void setCToJava(long pCToJava)
	{
		mCToJava = pCToJava;
	}
	
	public long getHostToGpu()
	{
		return mHostToGpu;
	}
	
	public void setHostToGpu(long pHostToGpu)
	{
		mHostToGpu = pHostToGpu;
	}
	
	public long getGpuToHost()
	{
		return mGpuToHost;
	}
	
	public void setGpuToHost(long pGpuToHost)
	{
		mGpuToHost = pGpuToHost;
	}
}
