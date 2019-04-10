package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;

public class TupleKindsToVariablesGeneratorMapper extends StringKeyMapper<KernelBuilder.IKernelVariablesGenerator>
{
	public TupleKindsToVariablesGeneratorMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}
