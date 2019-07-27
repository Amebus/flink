package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getIterableFromArgs;

public interface IPluginWithVarTypes extends IPluginWithExtra
{
	
	default String getIntType()
	{
		return PDAKernelBuilderPlugin.Defaults.VarTypes.INT;
	}
	default String getDoubleType()
	{
		return PDAKernelBuilderPlugin.Defaults.VarTypes.DOUBLE;
	}
	default String getStringType()
	{
		return PDAKernelBuilderPlugin.Defaults.VarTypes.STRING;
	}
	
	default String getIntLogicalType()
	{
		return PDAKernelBuilderPlugin.Defaults.LogicalVarTypes.INT;
	}
	default String getDoubleLogicalType()
	{
		return PDAKernelBuilderPlugin.Defaults.LogicalVarTypes.DOUBLE;
	}
	default String getStringLogicalType()
	{
		return PDAKernelBuilderPlugin.Defaults.LogicalVarTypes.STRING;
	}
	
	default Iterable<String> getTypes()
	{
		return getIterableFromArgs(
			getIntType(),
			getDoubleType(),
			getStringType());
	}
	
	default Iterable<String> getLogicalTypes()
	{
		return getIterableFromArgs(
			getIntLogicalType(),
			getDoubleLogicalType(),
			getStringLogicalType());
	}
}
