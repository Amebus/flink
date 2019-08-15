package org.apache.flink.streaming.api.ocl.common.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;

public class GenericValueMapper<K, V>
	extends GenericMapper<K, V, V>
{
	
	public GenericValueMapper()
	{
		super();
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
