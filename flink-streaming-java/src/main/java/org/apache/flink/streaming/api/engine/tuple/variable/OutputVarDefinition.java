package org.apache.flink.streaming.api.engine.tuple.variable;

import org.apache.flink.streaming.configuration.CTType;

public class OutputVarDefinition extends VarDefinition
{
	public OutputVarDefinition(CTType pType, int pIndex)
	{
		super(pType, pIndex);
	}
	
	@Override
	public String getName()
	{
		return getName("_r");
	}
	
	@Override
	public int getLength()
	{
		return getCType().getMaxByteOccupation();
	}
	
	@Override
	public boolean isInputVar()
	{
		return false;
	}
}
