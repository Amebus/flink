package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsensitiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilderPlugin;

public class TemplatePluginMapper
	extends StringKeyMapper<IKernelBuilderPlugin>
{
	public TemplatePluginMapper()
	{
		super(new StringKeyCaseInsensitiveComparer());
	}
}
