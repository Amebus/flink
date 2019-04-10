package org.apache.flink.streaming.api.ocl.common.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.DefaultMapperKeyComparer;
import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.IOptionableMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class GenericOptionableMapper<K, V, F, O>
	implements IOptionableMapper<K, V, F, O>
{
	private Map<IMapperKeyComparerWrapper<K>, F> mMap;
	private IMapperKeyComparerWrapper<K> mComparer;
	
	protected GenericOptionableMapper()
	{
		this(new DefaultMapperKeyComparer<>(null));
	}
	
	protected GenericOptionableMapper(IMapperKeyComparerWrapper<K> pComparer)
	{
		mComparer = pComparer;
		mMap = new HashMap<>();
	}
	
	protected F internalResolve(K pKey)
	{
		return mMap.get(mComparer.setValue(pKey));
	}
	
	protected F internalUnregister(K pKey)
	{
		return mMap.remove(mComparer.setValue(pKey));
	}
	
	@Override
	public Iterable<K> getKeys()
	{
		return mMap
			.keySet()
			.stream()
			.map(IMapperKeyComparerWrapper::getValue)
			.collect(Collectors.toList());
	}
	
	@Override
	public void register(K pKey, F pGetValueFunction)
	{
		mMap.put(mComparer.getNew(pKey), pGetValueFunction);
	}
	
	@Override
	public boolean containsKey(K pKey)
	{
		return mMap.containsKey(mComparer.setValue(pKey));
	}
	
	@Override
	public boolean isEmpty()
	{
		return mMap.isEmpty();
	}
	
	public IMapperKeyComparerWrapper<K> getComparer()
	{
		return mComparer;
	}
}
