package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.comparers.DefaultMapperKeyComparer;
import org.apache.flink.api.common.IMapper;
import org.apache.flink.api.common.IMapperKeyComparerWrapper;

import java.util.HashMap;
import java.util.Map;

public abstract class GenericMapper<K, V, F> implements IMapper<K, V, F>
{
	
	private Map<IMapperKeyComparerWrapper<K>, F> mMap;
	private IMapperKeyComparerWrapper<K> mComparer;
	
	protected GenericMapper()
	{
		this(new DefaultMapperKeyComparer<>(null));
	}
	
	protected GenericMapper(IMapperKeyComparerWrapper<K> pComparer)
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
