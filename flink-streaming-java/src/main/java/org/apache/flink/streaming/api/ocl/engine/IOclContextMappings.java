package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.*;

public interface IOclContextMappings
{
	FunctionKernelBuilderMapper getFunctionKernelBuilderMapper();
	
	FunctionKernelBuilderOptionMapper getFunctionKernelBuilderOptionMapper();
	
	StringKeyMapper<Byte> getVarTypeToSerializationTypeMapper();
	
	ITupleBytesDimensionGetters getTupleBytesDimensionGetters();
	
	
	
	
	PDAKernelBuilderMapper getKernelBuilderMapper();
	
	NumbersByteOrderingStreamWriterMapper getNumbersByteOrderingStreamWriterMapper();
	NumbersByteOrderingStreamReaderMapper getNumbersByteOrderingStreamReaderMapper();
	NumbersByteOrderingToIdentityValuesConverterMapper getByteOrderingToIdentityValuesConverterMapper();
}
