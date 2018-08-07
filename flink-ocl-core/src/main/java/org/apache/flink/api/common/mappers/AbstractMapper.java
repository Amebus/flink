package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapper;
import org.apache.flink.api.common.OnDemandLoader;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMapper<K, V, O> implements IMapper<K, V, O>
{
	
	private Map<K, OnDemandLoader<V, O>> mOnDemandLoaderMap;
	
	public AbstractMapper()
	{
		mOnDemandLoaderMap = new HashMap<>();
	}
	
	@Override
	public V resolve(K pKey, O pOptions)
	{
		return mOnDemandLoaderMap.get(pKey).get(pOptions);
	}
	
	@Override
	public void register(K pKey, OnDemandLoader<V, O> pOnDemandLoader)
	{
		mOnDemandLoaderMap.put(pKey, pOnDemandLoader);
	}
}
