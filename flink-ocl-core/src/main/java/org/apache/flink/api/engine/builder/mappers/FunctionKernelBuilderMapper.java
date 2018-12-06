package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.mappers.StringKeyOnDemandLoadMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderMapper extends StringKeyOnDemandLoadMapper<KernelBuilder, KernelBuilderOptions>
{
	public FunctionKernelBuilderMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
