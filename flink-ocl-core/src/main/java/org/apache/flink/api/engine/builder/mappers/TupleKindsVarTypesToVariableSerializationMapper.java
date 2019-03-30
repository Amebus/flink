package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;

public class TupleKindsVarTypesToVariableSerializationMapper extends StringKeyMapper<KernelBuilder.IVariableSerialization>
{
	public TupleKindsVarTypesToVariableSerializationMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}