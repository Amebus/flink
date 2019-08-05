package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.KernelBuilderMapper;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private CppLibraryInfo mCppLibraryInfo;
	private KernelBuilderMapper mKernelBuilderMapper;
	
	public BuildEngine(
		ISettingsRepository pSettingsRepository,
		KernelBuilderMapper pKernelBuilderMapper)
	{
		mSettingsRepository = pSettingsRepository;
		mKernelBuilderMapper = pKernelBuilderMapper;
	}
	
	public BuildEngine generateKernels(
		ITupleDefinitionRepository pTupleDefinitions,
		Iterable<? extends IUserFunction> pUserFunctions)
	{
		mCppLibraryInfo = new KernelCodeBuilderEngine(
			mSettingsRepository,
			pTupleDefinitions,
			pUserFunctions,
			mKernelBuilderMapper)
		.generateKernels();
		return this;
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
