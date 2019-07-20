package org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility;

public class KernelDeserializationLine
{
	private String mDeserLine;
	private int mDeserIndexOrder;
	
	public KernelDeserializationLine(String pDeserLine, int pDeserIndexOrder)
	{
		mDeserLine = pDeserLine;
		mDeserIndexOrder = pDeserIndexOrder;
	}
	
	public String getDeserLine()
	{
		return mDeserLine;
	}
	
	public int getDeserIndexOrder()
	{
		return mDeserIndexOrder;
	}
}
