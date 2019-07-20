package org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility;

public class KernelLogicalVariable
{
	private String mVarType;
	private String mVarName;
	private int mIndex;
	private int mBytesDim;
	
	public KernelLogicalVariable(String pVarType, String pVarName, int pIndex)
	{
		this(pVarType, pVarName, pIndex, 0);
	}
	
	public KernelLogicalVariable(String pVarType, String pVarName, int pIndex, int pBytesDim)
	{
		mVarType = pVarType;
		mVarName = pVarName;
		mIndex = pIndex;
		mBytesDim = pBytesDim;
	}
	
	public String getVarType()
	{
		return mVarType;
	}
	
	public String getVarName()
	{
		return mVarName;
	}
	
	public int getIndex()
	{
		return mIndex;
	}
	
	public int getBytesDim()
	{
		return mBytesDim;
	}
	
	public boolean isBytesDimSpecified()
	{
		return mBytesDim > 0;
	}
}
