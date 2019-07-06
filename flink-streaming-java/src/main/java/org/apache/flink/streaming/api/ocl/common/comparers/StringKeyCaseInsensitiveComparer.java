package org.apache.flink.streaming.api.ocl.common.comparers;

import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;

public class StringKeyCaseInsensitiveComparer extends GenericMapperKeyComparer<String>
{
	public StringKeyCaseInsensitiveComparer()
	{
		this("");
	}
	
	public StringKeyCaseInsensitiveComparer(String pValue)
	{
		super(pValue);
	}
	
	@Override
	public IMapperKeyComparerWrapper<String> getNew(String pValue)
	{
		return new StringKeyCaseInsensitiveComparer(pValue);
	}
	
	@Override
	public String getValueForComparison()
	{
		return getValue().toLowerCase();
	}
}

