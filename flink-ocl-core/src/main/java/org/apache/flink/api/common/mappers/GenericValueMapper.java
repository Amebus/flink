package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;

public class GenericValueMapper<K, V>
	extends GenericMapper<K, V, V>
{
	
	public GenericValueMapper()
	{
	}
	
	public GenericValueMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		super(pComparer);
	}
	
	@Override
	public V resolve(K pKey)
	{
		return internalResolve(pKey);
	}
	
	@Override
	public V unregister(K pKey)
	{
		return internalUnregister(pKey);
	}
}
