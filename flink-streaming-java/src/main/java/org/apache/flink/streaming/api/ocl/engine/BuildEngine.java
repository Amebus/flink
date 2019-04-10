package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private CppLibraryInfo mCppLibraryInfo;
	private FunctionKernelBuilderMapper mFunctionKernelBuilderMapper;
	private FunctionKernelBuilderOptionMapper mFunctionKernelBuilderOptionMapper;
	
	public BuildEngine(
		ISettingsRepository pSettingsRepository,
		FunctionKernelBuilderMapper pFunctionKernelBuilderMapper,
		FunctionKernelBuilderOptionMapper pFunctionKernelBuilderOptionMapper)
	{
		mSettingsRepository = pSettingsRepository;
		mFunctionKernelBuilderMapper = pFunctionKernelBuilderMapper;
		mFunctionKernelBuilderOptionMapper = pFunctionKernelBuilderOptionMapper;
	}
	
	public BuildEngine generateKernels(
		ITupleDefinitionRepository pTupleDefinitions,
		Iterable<? extends IUserFunction> pUserFunctions)
	{
		mCppLibraryInfo = new KernelCodeBuilderEngine(
			mSettingsRepository,
			pTupleDefinitions,
			pUserFunctions,
			mFunctionKernelBuilderMapper,
			mFunctionKernelBuilderOptionMapper)
			.generateKernels();
		return this;
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
