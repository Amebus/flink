package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsensitiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;

public class TupleKindsVarTypesToVariableSerializationMapper extends StringKeyMapper<KernelBuilder.IVariableSerialization>
{
	public TupleKindsVarTypesToVariableSerializationMapper()
	{
		super(new StringKeyCaseInsensitiveComparer(""));
	}
}
