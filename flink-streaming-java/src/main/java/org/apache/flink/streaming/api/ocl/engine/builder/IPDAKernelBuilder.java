package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.IBuilder;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;

public interface IPDAKernelBuilder extends IBuilder<OclKernel>
{
	IPDAKernelBuilder setPDAKernelBuilderOptions(PDAKernelBuilderOptions pPDAKernelBuilderOptions);
}
