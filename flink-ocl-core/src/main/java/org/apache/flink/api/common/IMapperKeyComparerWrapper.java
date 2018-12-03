package org.apache.flink.api.common;

public interface IMapperKeyComparerWrapper<K>
{
	K getValue();
	
	IMapperKeyComparerWrapper<K> setValue(K pValue);
	
	boolean equals(Object obj);
	
	int hashCode();
	
	IMapperKeyComparerWrapper<K> getNew(K pValue);
}
