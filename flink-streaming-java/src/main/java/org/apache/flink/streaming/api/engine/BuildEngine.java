package org.apache.flink.streaming.api.engine;

import org.apache.flink.streaming.api.engine.kernel.KernelCodeBuilderEngine;
import org.apache.flink.streaming.api.engine.kernel.OclKernel;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionsRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private Iterable<OclKernel> mKernels;
	private CppLibraryInfo mCppLibraryInfo;
	
	public BuildEngine(ISettingsRepository pSettingsRepository)
	{
		mSettingsRepository = pSettingsRepository;
	}
	
	public BuildEngine generateKernels(ITupleDefinitionsRepository pTupleDefinitions, Iterable<? extends IUserFunction> pUserFunctions)
	{
		mCppLibraryInfo = new KernelCodeBuilderEngine(mSettingsRepository, pTupleDefinitions, pUserFunctions).generateKernels();
		return this;
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
