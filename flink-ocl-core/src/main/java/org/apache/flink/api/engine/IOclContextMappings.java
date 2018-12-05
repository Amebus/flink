package org.apache.flink.api.engine;

import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;
import org.apache.flink.api.newEngine.kernel.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;

public interface IOclContextMappings
{
	//To remove
	FunctionKernelBuilderMapping getFunctionKernelBuilderMapping();
	
	FunctionKernelBuilderMapper getFunctionKernelBuilderMapper();
	
	FunctionKernelBuilderOptionMapper getFunctionKernelBuilderOptionMapper();
	
	StreamWriter getStreamWriter();
	
	StreamReader getStreamReader();
}
