package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;

public class TypeToKernelVariablesLineMapper extends StringKeyMapper<KernelBuilder.IKernelVariablesLineGenerator>
{
	public TypeToKernelVariablesLineMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}
