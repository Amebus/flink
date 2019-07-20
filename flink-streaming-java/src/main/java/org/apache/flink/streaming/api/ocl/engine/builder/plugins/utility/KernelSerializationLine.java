package org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility;

public class KernelSerializationLine
{
	private String mSerLine;
	private int mSerIndexOrder;
	
	public KernelSerializationLine(String pSerLine, int pSerIndexOrder)
	{
		mSerLine = pSerLine;
		mSerIndexOrder = pSerIndexOrder;
	}
	
	public String getSerLine()
	{
		return mSerLine;
	}
	
	public int getSerIndexOrder()
	{
		return mSerIndexOrder;
	}
}
