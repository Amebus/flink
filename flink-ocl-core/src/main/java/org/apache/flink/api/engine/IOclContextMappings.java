package org.apache.flink.api.engine;

import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;

public interface IOclContextMappings
{
	FunctionKernelBuilderMapping getFunctionKernelBuilderMapping();
}
