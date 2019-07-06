package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsensitiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;

public class TupleKindVarTypeToKernelTypeMapper extends StringKeyMapper<String>
{
	public TupleKindVarTypeToKernelTypeMapper()
	{
		super(new StringKeyCaseInsensitiveComparer(""));
	}
}
