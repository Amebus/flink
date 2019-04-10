package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyOnDemandLoadMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderMapper extends StringKeyOnDemandLoadMapper<KernelBuilder, KernelBuilderOptions>
{
	public FunctionKernelBuilderMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
