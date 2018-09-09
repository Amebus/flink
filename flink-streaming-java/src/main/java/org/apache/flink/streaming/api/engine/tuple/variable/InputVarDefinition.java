package org.apache.flink.streaming.api.engine.tuple.variable;

import org.apache.flink.streaming.configuration.CTType;

public class InputVarDefinition extends VarDefinition
{
	public InputVarDefinition(CTType pType, int pIndex)
	{
		super(pType, pIndex);
	}
	
	@Override
	public String getName()
	{
		return getName("_t");
	}
	
	@Override
	public int getLength()
	{
		return getCType().getByteOccupation();
	}
	
	@Override
	public boolean isInputVar()
	{
		return true;
	}
}
