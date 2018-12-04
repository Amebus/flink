package org.apache.flink.api.engine;

import org.apache.flink.api.engine.kernel.KernelCodeBuilderEngine;
import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private CppLibraryInfo mCppLibraryInfo;
	private FunctionKernelBuilderMapping mFunctionKernelBuilderMapping;
	
	public BuildEngine(ISettingsRepository pSettingsRepository, FunctionKernelBuilderMapping pFunctionKernelBuilderMapping)
	{
		mSettingsRepository = pSettingsRepository;
		mFunctionKernelBuilderMapping = pFunctionKernelBuilderMapping;
	}
	
	public BuildEngine generateKernels(
		ITupleDefinitionsRepository pTupleDefinitions,
		Iterable<? extends IUserFunction> pUserFunctions)
	{
		mCppLibraryInfo = new KernelCodeBuilderEngine(
			mSettingsRepository,
			pTupleDefinitions,
			pUserFunctions,
			mFunctionKernelBuilderMapping)
			.generateKernels();
		return this;
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
