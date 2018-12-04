package org.apache.flink.api.common;

public interface IMapper<K, V, F>
{
	
	boolean containsKey(K pKey);
	
	V resolve(K pKey);
	
	void register(K pKey, F pSupplier);
	
}
