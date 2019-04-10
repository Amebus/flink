package org.apache.flink.streaming.api.ocl.common.comparers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;

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
