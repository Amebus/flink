package org.apache.flink.api.engine;

import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.api.engine.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;

public interface IOclContextMappings
{
	FunctionKernelBuilderMapper getFunctionKernelBuilderMapper();
	
	FunctionKernelBuilderOptionMapper getFunctionKernelBuilderOptionMapper();
	
	StringKeyMapper<Byte> getVarTypeToSerializationTypeMapper();
	
	ITupleBytesDimensionGetters getTupleBytesDimensionGetters();
	
	StreamWriter getStreamWriter();
	
	StreamReader getStreamReader();
}
