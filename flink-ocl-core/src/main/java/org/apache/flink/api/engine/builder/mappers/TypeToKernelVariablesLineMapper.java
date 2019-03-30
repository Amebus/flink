package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;

public class TypeToKernelVariablesLineMapper extends StringKeyMapper<KernelBuilder.IKernelVariablesLineGenerator>
{
	public TypeToKernelVariablesLineMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}