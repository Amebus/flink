package org.apache.flink.api.common.comparers;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;

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

