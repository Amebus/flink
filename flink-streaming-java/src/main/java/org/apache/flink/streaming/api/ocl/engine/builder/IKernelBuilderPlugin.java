package org.apache.flink.streaming.api.ocl.engine.builder;

public interface IKernelBuilderPlugin
{
	void parseTemplateCode(
		KernelBuilder pKernelBuilder,
		StringBuilder pCodeBuilder);
}
