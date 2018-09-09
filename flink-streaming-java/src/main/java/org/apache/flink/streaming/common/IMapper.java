package org.apache.flink.streaming.common;

public interface IMapper<K, V, O>
{
	
	V resolve(K pKey, O pOptions);
	
	void register(K pKey, OnDemandLoader<V, O> pOnDemandLoaderContainer);
	
}
