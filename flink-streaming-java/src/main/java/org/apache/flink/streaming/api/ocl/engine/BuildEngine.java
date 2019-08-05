package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.PDAKernelBuilderMapper;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private CppLibraryInfo mCppLibraryInfo;
	private PDAKernelBuilderMapper mKernelBuilderMapper;
	
	private FunctionKernelBuilderMapper mFunctionKernelBuilderMapper;
	private FunctionKernelBuilderOptionMapper mFunctionKernelBuilderOptionMapper;
	
	public BuildEngine(
		ISettingsRepository pSettingsRepository,
		PDAKernelBuilderMapper pKernelBuilderMapper)
	{
		mSettingsRepository = pSettingsRepository;
		mKernelBuilderMapper = pKernelBuilderMapper;
	}
	
	public BuildEngine generateKernels(
		ITupleDefinitionRepository pTupleDefinitions,
		Iterable<? extends IUserFunction> pUserFunctions)
	{
		mCppLibraryInfo = new PDAKernelCodeBuilderEngine(
			mSettingsRepository,
			pTupleDefinitions,
			pUserFunctions,
			mKernelBuilderMapper)
		.generateKernels();
		return this;
	}
	
//	public BuildEngine(
//		ISettingsRepository pSettingsRepository,
//		FunctionKernelBuilderMapper pFunctionKernelBuilderMapper,
//		FunctionKernelBuilderOptionMapper pFunctionKernelBuilderOptionMapper)
//	{
//		mSettingsRepository = pSettingsRepository;
//		mFunctionKernelBuilderMapper = pFunctionKernelBuilderMapper;
//		mFunctionKernelBuilderOptionMapper = pFunctionKernelBuilderOptionMapper;
//	}
//
//	public BuildEngine generateKernels(
//		ITupleDefinitionRepository pTupleDefinitions,
//		Iterable<? extends IUserFunction> pUserFunctions)
//	{
//		mCppLibraryInfo = new KernelCodeBuilderEngine(
//			mSettingsRepository,
//			pTupleDefinitions,
//			pUserFunctions,
//			mFunctionKernelBuilderMapper,
//			mFunctionKernelBuilderOptionMapper)
//		.generateKernels();
//		return this;
//	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
