package org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce;

import org.apache.flink.streaming.api.ocl.engine.builder.plugins.DeserializationPlugin;

public class DeserializationAPlugin extends DeserializationPlugin
{
	@Override
	protected String getInputLogicalVarsKey()
	{
		return "input-logical-a-vars";
	}
	@Override
	protected String getDataVariableName()
	{
		return "_localCache";
	}
	@Override
	protected String getIndexVariableNAme()
	{
		return "_iTemp";
	}
}
