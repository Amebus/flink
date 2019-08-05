package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.IPDAKernelBuilder;

public class PDAKernelBuilderMapper extends StringKeyMapper<IPDAKernelBuilder>
{
	public PDAKernelBuilderMapper()
	{
	}
	
	public PDAKernelBuilderMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
