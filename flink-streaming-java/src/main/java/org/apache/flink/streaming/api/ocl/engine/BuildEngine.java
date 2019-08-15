package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.KernelBuilderMapper;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class BuildEngine
{
	private ISettingsRepository mSettingsRepository;
	private CppLibraryInfo mCppLibraryInfo;
	private ITupleBytesDimensionGetter mTupleBytesDimensionGetter;
	private KernelBuilderMapper mKernelBuilderMapper;
	
	public BuildEngine(
		ISettingsRepository pSettingsRepository,
		ITupleBytesDimensionGetter pTupleBytesDimensionGetter,
		KernelBuilderMapper pKernelBuilderMapper)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleBytesDimensionGetter = pTupleBytesDimensionGetter;
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
			mTupleBytesDimensionGetter,
			mKernelBuilderMapper)
		.generateKernels();
		return this;
	}
	
	public CppLibraryInfo getCppLibraryInfo()
	{
		return mCppLibraryInfo;
	}
}
