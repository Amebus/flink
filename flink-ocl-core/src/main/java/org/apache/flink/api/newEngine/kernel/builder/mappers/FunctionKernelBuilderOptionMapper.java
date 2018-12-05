package org.apache.flink.api.newEngine.kernel.builder.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.newEngine.kernel.builder.IKernelBuilderOptionsBuilder;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderOptionMapper
	extends StringKeyMapper<IKernelBuilderOptionsBuilder<? extends KernelBuilderOptions>>
{
	public FunctionKernelBuilderOptionMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
