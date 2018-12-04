package org.apache.flink.api.newConfiguration.tuple;

import org.apache.flink.api.newConfiguration.ITupleVarDefinition;

public abstract class GenericTupleVarDefinition implements ITupleVarDefinition
{
	private String mType;
	private int mMaxReservedBytes;
	private Object mIdentityValue;
	
	protected GenericTupleVarDefinition(String pType, int pMaxReservedBytes, Object pIdentityValue)
	{
		setType(pType);
		setMaxReservedBytes(pMaxReservedBytes);
		setIdentityValue(pIdentityValue);
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
}
