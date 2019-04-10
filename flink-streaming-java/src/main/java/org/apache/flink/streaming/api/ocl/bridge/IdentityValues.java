package org.apache.flink.streaming.api.ocl.bridge;

import org.apache.flink.streaming.configuration.ITupleDefinition;

public class IdentityValues
{
	private ITupleDefinition mTuple;
	private int mTupleDimension;
	
	public IdentityValues(ITupleDefinition pTuple, int pTupleDimension)
	{
		mTuple = pTuple;
		mTupleDimension = pTupleDimension;
	}
	
	public ITupleDefinition getTuple()
	{
		return mTuple;
	}
	
	public int getTupleDimension()
	{
		return mTupleDimension;
	}
}
