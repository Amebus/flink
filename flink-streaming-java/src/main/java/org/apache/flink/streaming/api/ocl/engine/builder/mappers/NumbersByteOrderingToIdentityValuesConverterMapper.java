package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.bridge.identity.IdentityValueToIdentityArrayConverter;
import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.mappers.GenericValueMapper;

import java.nio.ByteOrder;

public class NumbersByteOrderingToIdentityValuesConverterMapper
	extends GenericValueMapper<ByteOrder, IdentityValueToIdentityArrayConverter>
{
	public NumbersByteOrderingToIdentityValuesConverterMapper()
	{
	}
	
	public NumbersByteOrderingToIdentityValuesConverterMapper(IMapperKeyComparerWrapper<ByteOrder> pComparer)
	{
		super(pComparer);
	}
}
