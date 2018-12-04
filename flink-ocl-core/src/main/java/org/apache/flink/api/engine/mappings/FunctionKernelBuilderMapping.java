package org.apache.flink.api.engine.mappings;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.mappers.StringKeyOnDemandLoadMapper;
import org.apache.flink.api.engine.kernel.builder.KernelBuilder;
import org.apache.flink.api.engine.kernel.builder.KernelBuilderOptions;

public abstract class FunctionKernelBuilderMapping extends StringKeyOnDemandLoadMapper<KernelBuilder, KernelBuilderOptions>
{
	public FunctionKernelBuilderMapping(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
