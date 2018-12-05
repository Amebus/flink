package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;

public class StringKeyMapper<V> extends GenericMapper<String, V, V>
{
	public StringKeyMapper()
	{
		super();
	}
	
	public StringKeyMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
	
	@Override
	public V unregister(String pKey)
	{
		return internalUnregister(pKey);
	}
	
	@Override
	public V resolve(String pKey)
	{
		return internalResolve(pKey);
	}
}
