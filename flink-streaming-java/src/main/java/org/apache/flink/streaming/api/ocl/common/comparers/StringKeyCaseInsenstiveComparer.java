package org.apache.flink.streaming.api.ocl.common.comparers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;

public class StringKeyCaseInsenstiveComparer extends GenericMapperKeyComparer<String>
{
	public StringKeyCaseInsenstiveComparer(String pValue)
	{
		super(pValue);
	}
	
	@Override
	public IMapperKeyComparerWrapper<String> getNew(String pValue)
	{
		return new StringKeyCaseInsenstiveComparer(pValue);
	}
	
	@Override
	public String getValueForComparison()
	{
		return getValue().toLowerCase();
	}
}

