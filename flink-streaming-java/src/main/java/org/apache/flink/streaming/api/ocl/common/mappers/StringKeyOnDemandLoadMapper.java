package org.apache.flink.streaming.api.ocl.common.mappers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;

public class StringKeyOnDemandLoadMapper<T, O> extends GenericOptionableOnDemandLoadMapper<String, T, O>
{
	
	public StringKeyOnDemandLoadMapper()
	{
		super();
	}
	
	public StringKeyOnDemandLoadMapper(IMapperKeyComparerWrapper<String> pComparer)
	{
		super(pComparer);
	}
}
