package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

public class PDAKernelBuilderOptions
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionsRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	private StringKeyMapper<Object> mExtras;
	
	public PDAKernelBuilderOptions()
	{
		this(null, null, null, null);
	}
	
	public PDAKernelBuilderOptions(IUserFunction pUserFunction, ITupleDefinitionRepository pTupleDefinitionsRepository, IOclContextOptions pContextOptions, IOclKernelsOptions pKernelOptions)
	{
		mUserFunction = pUserFunction;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mContextOptions = pContextOptions;
		mKernelOptions = pKernelOptions;
		mExtras = new StringKeyMapper<>();
	}
	
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	public PDAKernelBuilderOptions setUserFunction(IUserFunction pUserFunction)
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
	
	public PDAKernelBuilderOptions setTupleDefinitionsRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
	{
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		return this;
	}
	
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	
	public PDAKernelBuilderOptions setContextOptions(IOclContextOptions pContextOptions)
	{
		mContextOptions = pContextOptions;
		return this;
	}
	
	public IOclKernelsOptions getKernelOptions()
	{
		return mKernelOptions;
	}
	
	public PDAKernelBuilderOptions setKernelOptions(IOclKernelsOptions pKernelOptions)
	{
		mKernelOptions = pKernelOptions;
		return this;
	}
	
	protected StringKeyMapper<Object> getExtrasContainer()
	{
		return mExtras;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getExtra(String pKey)
	{
		return (T) getExtrasContainer().resolve(pKey);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T removeExtra(String pKey)
	{
		return (T) getExtrasContainer().unregister(pKey);
	}
	
	public PDAKernelBuilderOptions setExtra(String pKey, Object pExtra)
	{
		getExtrasContainer().register(pKey, pExtra);
		return this;
	}
	
	public PDAKernelBuilderOptions clearExtras()
	{
		getExtrasContainer().clear();
		return this;
	}
}
