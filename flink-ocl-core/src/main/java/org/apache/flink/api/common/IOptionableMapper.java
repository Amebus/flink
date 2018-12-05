package org.apache.flink.api.common;

public interface IOptionableMapper<K, V, F, O>
{
	V resolve(K pKey, O pOptions);
	
	void register(K pKey, F pGetValueFunction);
	
	void unregister(K pKey);
	
	V unregister(K pKey, O pOptions);
	
	Iterable<K> getKeys();
	
	boolean containsKey(K pKey);
	
	boolean isEmpty();
}
