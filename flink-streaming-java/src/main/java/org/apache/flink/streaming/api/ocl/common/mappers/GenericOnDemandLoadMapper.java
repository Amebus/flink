package org.apache.flink.streaming.api.ocl.common.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.ISupplier;

public class GenericOnDemandLoadMapper<K, V> extends GenericMapper<K, V, ISupplier<V>>
{
	protected GenericOnDemandLoadMapper()
	{
		super();
	}
	
	protected GenericOnDemandLoadMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		super(pComparer);
	}
	
	@Override
	public V unregister(K pKey)
	{
		return internalUnregister(pKey).get();
	}
	
	@Override
	public V resolve(K pKey)
	{
		return internalResolve(pKey).get();
	}
}
