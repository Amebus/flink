package org.apache.flink.api.engine.builder;

import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.configuration.ITupleDefinitionRepository;

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
