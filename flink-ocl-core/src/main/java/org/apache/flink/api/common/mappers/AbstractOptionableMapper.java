package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.comparers.DefaultMapperKeyComparer;
import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.IOptionableMapper;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractOptionableMapper<K, V, F, O>
	implements IOptionableMapper<K, V, F, O>
{
	private Map<IMapperKeyComparerWrapper<K>, F> mMap;
	private IMapperKeyComparerWrapper<K> mComparer;
	
	protected AbstractOptionableMapper()
	{
		this(new DefaultMapperKeyComparer<>(null));
	}
	
	protected AbstractOptionableMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		mComparer = pComparer;
		mMap = new HashMap<>();
	}
	
	protected F internalResolve(K pKey)
	{
		return mMap.get(mComparer.setValue(pKey));
	}
	
	@Override
	public void register(K pKey, F pGetValueFunction)
	{
		mMap.put(mComparer.getNew(pKey), pGetValueFunction);
	}
}
