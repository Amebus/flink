package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;

public class TupleKindsToVariablesGeneratorMapper extends StringKeyMapper<KernelBuilder.IKernelVariablesGenerator>
{
	public TupleKindsToVariablesGeneratorMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}
