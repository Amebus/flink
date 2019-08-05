package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilder;

public class KernelBuilderMapper extends StringKeyMapper<IKernelBuilder>
{
	public KernelBuilderMapper()
	{
	}
	
	public KernelBuilderMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
