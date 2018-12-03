package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.IOptionableSupplier;

public class AbstractOptionableOnDemandLoadMapper<K, V, O> extends AbstractOptionableMapper<K, V, IOptionableSupplier<V, O>, O>
{
	protected AbstractOptionableOnDemandLoadMapper()
	{
		super();
	}
	
	protected AbstractOptionableOnDemandLoadMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		super(pComparer);
	}
	
	@Override
	public V resolve(K pKey, O pOptions)
	{
		return internalResolve(pKey).get(pOptions);
	}
}
