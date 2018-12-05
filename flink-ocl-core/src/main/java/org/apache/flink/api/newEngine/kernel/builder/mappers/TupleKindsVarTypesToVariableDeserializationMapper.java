package org.apache.flink.api.newEngine.kernel.builder.mappers;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilder;

public class TupleKindsVarTypesToVariableDeserializationMapper extends StringKeyMapper<KernelBuilder.IVariableDeserialization>
{
	public TupleKindsVarTypesToVariableDeserializationMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}
