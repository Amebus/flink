package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilderOptionsBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderOptionMapper
	extends StringKeyMapper<IKernelBuilderOptionsBuilder<? extends KernelBuilderOptions>>
{
	public FunctionKernelBuilderOptionMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
