package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.engine.KernelBuilderOptions;

public class ReduceBuilder extends KernelWithoutOutputTupleBuilder
{
	public ReduceBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
}
