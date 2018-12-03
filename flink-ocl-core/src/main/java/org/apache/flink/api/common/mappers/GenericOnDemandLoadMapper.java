package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.ISupplier;

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
	public V resolve(K pKey)
	{
		return internalResolve(pKey).get();
	}
}
