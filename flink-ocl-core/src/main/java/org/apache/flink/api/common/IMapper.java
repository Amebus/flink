package org.apache.flink.api.common;

public interface IMapper<K, V, F>
{
	
	V resolve(K pKey);
	
	void register(K pKey, F pSupplier);
	
}
