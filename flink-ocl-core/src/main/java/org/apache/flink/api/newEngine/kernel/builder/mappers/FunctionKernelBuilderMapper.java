package org.apache.flink.api.newEngine.kernel.builder.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.mappers.StringKeyOnDemandLoadMapper;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilder;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderMapper extends StringKeyOnDemandLoadMapper<KernelBuilder, KernelBuilderOptions>
{
	public FunctionKernelBuilderMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
