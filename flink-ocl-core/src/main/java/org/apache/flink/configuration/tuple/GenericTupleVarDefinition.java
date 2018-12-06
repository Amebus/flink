package org.apache.flink.configuration.tuple;

import org.apache.flink.configuration.ITupleVarDefinition;

public abstract class GenericTupleVarDefinition implements ITupleVarDefinition
{
	private String mType;
	private int mMaxReservedBytes;
	private Object mIdentityValue;
	private int mIndex;
	
	protected GenericTupleVarDefinition(
		String pType,
		int pMaxReservedBytes,
		Object pIdentityValue,
		int pIndex)
	{
		setType(pType);
		setMaxReservedBytes(pMaxReservedBytes);
		setIdentityValue(pIdentityValue);
		setIndex(pIndex);
	}
	
	protected void setType(String pType)
	{
		mType = pType;
	}
	protected void setMaxReservedBytes(int pMaxReservedBytes)
	{
		mMaxReservedBytes = pMaxReservedBytes;
	}
	protected void setIdentityValue(Object pIdentityValue)
	{
		mIdentityValue = pIdentityValue;
	}
	protected void setIndex(int pIndex)
	{
		mIndex = pIndex;
	}
	
	@Override
	public String getType()
	{
		return mType;
	}
	
	@Override
	public int getMaxReservedBytes()
	{
		return mMaxReservedBytes;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <T> T getIdentityValue()
	{
		return (T) mIdentityValue;
	}
	
	@Override
	public boolean hasIdentityValue()
	{
		return mIdentityValue != null;
	}
	
	@Override
	public int getIndex()
	{
		return 0;
	}
}
