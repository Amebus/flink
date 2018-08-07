package org.apache.flink.api.engine;

import org.apache.flink.configuration.CTType;

public abstract class VarDefinition
{
	private CTType mType;
	private int mIndex;
	
	public VarDefinition(CTType pType, int pIndex)
	{
		mType = pType;
		mIndex = pIndex;
	}
	
	public String getType()
	{
		return mType.getT();
	}
	
	public CTType getCType()
	{
		return mType;
	}
	
	public abstract String getName();
	protected String getName(String pPrefix)
	{
		return pPrefix + getIndex();
	}
	
	public abstract int getLength();
	
	public int getIndex()
	{
		return mIndex;
	}
	
	public abstract boolean isInputVar();
	
	public boolean isOutputvar()
	{
		return !isInputVar();
	}
}
