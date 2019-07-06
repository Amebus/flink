package org.apache.flink.streaming.api.ocl.engine.builder;

public interface IPDAKernelBuilderPlugin
{
	void parseTemplateCode(
		PDAKernelBuilder pKernelBuilder,
		StringBuilder pCodeBuilder);
}
