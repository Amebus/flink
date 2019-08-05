package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.IBuilder;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;

public interface IKernelBuilder extends IBuilder<OclKernel>
{
	IKernelBuilder setPDAKernelBuilderOptions(KernelBuilderOptions pKernelBuilderOptions);
}
