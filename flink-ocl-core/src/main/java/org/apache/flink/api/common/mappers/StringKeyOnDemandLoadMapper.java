package org.apache.flink.api.common.mappers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;

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
