package org.apache.flink.api.common;

public interface IOptionableMapper<K, V, F, O>
{
	boolean containsKey(K pKey);
	
	V resolve(K pKey, O pOptions);
	
	void register(K pKey, F pGetValueFunction);
}
