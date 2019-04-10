package org.apache.flink.streaming.api.ocl.common;

public interface IMapperKeyComparerWrapper<K>
{
	K getValueForComparison();
	
	K getValue();
	IMapperKeyComparerWrapper<K> setValue(K pValue);
	
	IMapperKeyComparerWrapper<K> getNew(K pValue);
}
