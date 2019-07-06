package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsensitiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.IPDAKernelBuilderPlugin;

public class TemplatePluginMapper
	extends StringKeyMapper<IPDAKernelBuilderPlugin>
{
	public TemplatePluginMapper()
	{
		super(new StringKeyCaseInsensitiveComparer());
	}
}
