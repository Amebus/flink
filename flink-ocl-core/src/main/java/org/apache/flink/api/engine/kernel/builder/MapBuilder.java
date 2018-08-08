package org.apache.flink.api.engine.kernel.builder;

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
