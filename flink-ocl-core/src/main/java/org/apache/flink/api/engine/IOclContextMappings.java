package org.apache.flink.api.engine;

import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;

public interface IOclContextMappings
{
	FunctionKernelBuilderMapping getFunctionKernelBuilderMapping();
	
	StreamWriter getStreamWriter();
	
	StreamReader getStreamReader();
}
