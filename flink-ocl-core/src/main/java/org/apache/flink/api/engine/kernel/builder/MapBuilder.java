package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.engine.KernelBuilderOptions;

public class MapBuilder extends KernelWithOutputTupleBuilder
{
	
	public MapBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	@Override
	protected String getOutputSection()
	{
		return null;
	}
	
}
