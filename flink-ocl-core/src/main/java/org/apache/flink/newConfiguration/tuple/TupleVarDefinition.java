package org.apache.flink.api.newConfiguration.tuple;

import org.apache.flink.api.newConfiguration.ITupleVarDefinition;

public class TupleVarDefinition extends GenericTupleVarDefinition
{
	public TupleVarDefinition(String pType, int pMaxReservedBytes, Object pIdentityValue)
	{
		super(pType, pMaxReservedBytes, pIdentityValue);
	}
	
	public TupleVarDefinition(ITupleVarDefinition pTupleVarDefinition)
	{
		this(pTupleVarDefinition.getType(),
			 pTupleVarDefinition.getMaxReservedBytes(),
			 pTupleVarDefinition.getIdentityValue());
	}
}
