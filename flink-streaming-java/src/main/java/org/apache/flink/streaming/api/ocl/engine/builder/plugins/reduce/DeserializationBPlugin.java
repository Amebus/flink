package org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce;

import org.apache.flink.streaming.api.ocl.engine.builder.plugins.DeserializationPlugin;

public class DeserializationBPlugin extends DeserializationPlugin
{
	@Override
	protected String getInputLogicalVarsKey()
	{
		return "input-logical-b-vars";
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
