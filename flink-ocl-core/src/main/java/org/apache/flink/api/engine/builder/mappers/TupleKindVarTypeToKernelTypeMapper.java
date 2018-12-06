package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;

public class TupleKindVarTypeToKernelTypeMapper extends StringKeyMapper<String>
{
	public TupleKindVarTypeToKernelTypeMapper()
	{
		super(new StringKeyCaseInsenstiveComparer(""));
	}
}
