package org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce;

import org.apache.flink.streaming.api.ocl.engine.builder.plugins.SerializationPlugin;

public class SerializeToLocalPlugin extends SerializationPlugin
{
	@Override
	protected String getOutputLogicalVarsKey()
	{
		return "input-logical-a-vars";
	}
	@Override
	protected String getResultVariableName()
	{
		return "_localCache";
	}
	@Override
	protected String getIndexVariableName()
	{
		return "_iTemp";
	}
	@Override
	protected String getStringLengthVarPrefix()
	{
		return "_rsl";
	}
}
