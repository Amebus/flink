package org.apache.flink.streaming.api.ocl.common.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.IOptionableSupplier;

public class GenericOptionableOnDemandLoadMapper<K, V, O> extends GenericOptionableMapper<K, V, IOptionableSupplier<V, O>, O>
{
	protected GenericOptionableOnDemandLoadMapper()
	{
		super();
	}
	
	protected GenericOptionableOnDemandLoadMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		super(pComparer);
	}
	
	@Override
	public void unregister(K pKey)
	{
		internalUnregister(pKey);
	}
	
	@Override
	public V unregister(K pKey, O pOptions)
	{
		return internalUnregister(pKey).get(pOptions);
	}
	
	@Override
	public V resolve(K pKey, O pOptions)
	{
		return internalResolve(pKey).get(pOptions);
	}
}
