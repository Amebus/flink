package org.apache.flink.api.engine.tuple.variable;

import org.apache.flink.api.engine.VarDefinition;
import org.apache.flink.configuration.CTType;

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
		return getCType().getMaxByteDimension();
	}
	
	@Override
	public boolean isInputVar()
	{
		return false;
	}
}
