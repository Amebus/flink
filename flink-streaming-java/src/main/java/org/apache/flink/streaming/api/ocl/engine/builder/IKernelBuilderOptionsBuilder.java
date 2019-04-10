package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.IBuilder;
import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public interface IKernelBuilderOptionsBuilder<T extends KernelBuilderOptions> extends IBuilder<T>
{
	
	IUserFunction getUserFunction();
	
	ITupleDefinitionRepository getTupleDefinitionRepository();
	
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getKernelOptions();
	
	
	IKernelBuilderOptionsBuilder<T> setUserFunction(IUserFunction pUserFunction);
	
	IKernelBuilderOptionsBuilder<T> setTupleDefinitionRepository(ITupleDefinitionRepository pTupleDefinitionsRepository);
	
	IKernelBuilderOptionsBuilder<T> setContextOptions(IOclContextOptions pContextOptions);
	
	IKernelBuilderOptionsBuilder<T> setKernelOptions(IOclKernelsOptions pKernelOptions);
}
