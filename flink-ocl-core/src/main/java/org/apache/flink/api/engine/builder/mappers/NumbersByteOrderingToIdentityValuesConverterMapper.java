package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.bridge.identity.IdentityValueToIdentityArrayConverter;
import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.mappers.GenericValueMapper;

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
