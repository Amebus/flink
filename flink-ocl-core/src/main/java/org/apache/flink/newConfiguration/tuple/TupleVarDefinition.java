package org.apache.flink.newConfiguration.tuple;

import org.apache.flink.newConfiguration.ITupleVarDefinition;

public class TupleVarDefinition extends GenericTupleVarDefinition
{
	public TupleVarDefinition(String pType, int pMaxReservedBytes, Object pIdentityValue, int pIndex)
	{
		super(pType, pMaxReservedBytes, pIdentityValue, pIndex);
	}
	
	public TupleVarDefinition(ITupleVarDefinition pTupleVarDefinition)
	{
		this(pTupleVarDefinition.getType(),
			 pTupleVarDefinition.getMaxReservedBytes(),
			 pTupleVarDefinition.getIdentityValue(),
			 pTupleVarDefinition.getIndex());
	}
}
