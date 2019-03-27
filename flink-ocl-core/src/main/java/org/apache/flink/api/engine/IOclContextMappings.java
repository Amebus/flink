package org.apache.flink.api.engine;

import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.builder.mappers.*;

public interface IOclContextMappings
{
	FunctionKernelBuilderMapper getFunctionKernelBuilderMapper();
	
	FunctionKernelBuilderOptionMapper getFunctionKernelBuilderOptionMapper();
	
	StringKeyMapper<Byte> getVarTypeToSerializationTypeMapper();
	
	ITupleBytesDimensionGetters getTupleBytesDimensionGetters();
	
	NumbersByteOrderingStreamWriterMapper getNumbersByteOrderingStreamWriterMapper();
	
	NumbersByteOrderingStreamReaderMapper getNumbersByteOrderingStreamReaderMapper();
	
	NumbersByteOrderingToIdentityValuesConverterMapper getByteOrderingToIdentityValuesConverterMapper();
	
}
