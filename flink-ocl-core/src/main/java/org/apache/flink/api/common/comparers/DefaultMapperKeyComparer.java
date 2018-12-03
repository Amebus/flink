package org.apache.flink.api.common.comparers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;

import java.util.HashMap;

public class DefaultMapperKeyComparer<K> extends GenericMapperKeyComparer<K>
{
	public DefaultMapperKeyComparer(K pValue)
	{
		super(pValue);
	}
	
	@Override
	public IMapperKeyComparerWrapper<K> getNew(K pValue)
	{
		return new DefaultMapperKeyComparer<>(pValue);
	}
}
