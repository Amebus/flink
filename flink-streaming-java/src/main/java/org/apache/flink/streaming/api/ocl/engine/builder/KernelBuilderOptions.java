package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.ITupleBytesDimensionGetter;
import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class KernelBuilderOptions
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionsRepository;
	private ITupleBytesDimensionGetter mTupleBytesDimensionGetter;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	public KernelBuilderOptions(
		IUserFunction pUserFunction,
		ITupleDefinitionRepository pTupleDefinitionsRepository,
		ITupleBytesDimensionGetter pTupleBytesDimensionGetter,
		IOclContextOptions pContextOptions,
		IOclKernelsOptions pKernelOptions)
	{
		mUserFunction = pUserFunction;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mTupleBytesDimensionGetter = pTupleBytesDimensionGetter;
		mContextOptions = pContextOptions;
		mKernelOptions = pKernelOptions;
	}
	
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	public KernelBuilderOptions setUserFunction(IUserFunction pUserFunction)
	{
		mUserFunction = pUserFunction;
		return this;
	}
	
	public boolean isInputTupleSpecified()
	{
		return getUserFunction().isInputTupleSpecified();
	}
	public ITupleDefinition getInputTuple()
	{
		return getTupleDefinitionsRepository().getTupleDefinition(getUserFunction().getInputTupleName());
	}
	
	public boolean isOutputTupleSpecified()
	{
		return getUserFunction().isOutputTupleSpecified();
	}
	public ITupleDefinition getOutputTuple()
	{
		IUserFunction vFunction = getUserFunction();
		if(vFunction.isOutputTupleSpecified())
			return getTupleDefinitionsRepository().getTupleDefinition(vFunction.getOutputTupleName());
		return null;
	}
	
	public ITupleDefinitionRepository getTupleDefinitionsRepository()
	{
		return mTupleDefinitionsRepository;
	}
	public KernelBuilderOptions setTupleDefinitionsRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
	{
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		return this;
	}
	
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	public KernelBuilderOptions setContextOptions(IOclContextOptions pContextOptions)
	{
		mContextOptions = pContextOptions;
		return this;
	}
	
	public IOclKernelsOptions getKernelOptions()
	{
		return mKernelOptions;
	}
	public KernelBuilderOptions setKernelOptions(IOclKernelsOptions pKernelOptions)
	{
		mKernelOptions = pKernelOptions;
		return this;
	}
	
	public ITupleBytesDimensionGetter getTupleBytesDimensionGetter()
	{
		return mTupleBytesDimensionGetter;
	}
	public KernelBuilderOptions setTupleBytesDimensionGetter(ITupleBytesDimensionGetter pTupleBytesDimensionGetter)
	{
		mTupleBytesDimensionGetter = pTupleBytesDimensionGetter;
		return this;
	}
}
