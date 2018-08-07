package org.apache.flink.api.engine;

import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;

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
	
	public void loadCppLibrary()
	{
//		System.loadLibrary("OCL");
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
