package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.NumbersByteOrderingStreamReaderMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.NumbersByteOrderingStreamWriterMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.NumbersByteOrderingToIdentityValuesConverterMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.PDAKernelBuilderMapper;

public interface IOclContextMappings
{
	
	ITupleBytesDimensionGetter getTupleBytesDimensionGetters();
	
	
	
	
	PDAKernelBuilderMapper getKernelBuilderMapper();
	
	NumbersByteOrderingStreamWriterMapper getNumbersByteOrderingStreamWriterMapper();
	NumbersByteOrderingStreamReaderMapper getNumbersByteOrderingStreamReaderMapper();
	NumbersByteOrderingToIdentityValuesConverterMapper getByteOrderingToIdentityValuesConverterMapper();
}
