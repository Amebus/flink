package org.apache.flink.api.newEngine.kernel.builder;

import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.newConfiguration.ITupleDefinitionRepository;

public class KernelBuilderOptions
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionsRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	public KernelBuilderOptions(
		IUserFunction pUserFunction,
		ITupleDefinitionRepository pTupleDefinitionsRepository,
		IOclContextOptions pContextOptions,
		IOclKernelsOptions pKernelOptions)
	{
		mUserFunction = pUserFunction;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mContextOptions = pContextOptions;
		mKernelOptions = pKernelOptions;
	}
	
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	public ITupleDefinitionRepository getTupleDefinitionsRepository()
	{
		return mTupleDefinitionsRepository;
	}
	
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	
	public IOclKernelsOptions getKernelOptions()
	{
		return mKernelOptions;
	}
	
	public static class KernelOptionsBuilder implements IBuilder<KernelBuilderOptions>
	{
		private IUserFunction mUserFunction;
		private ITupleDefinitionRepository mTupleDefinitionsRepository;
		private IOclContextOptions mContextOptions;
		private IOclKernelsOptions mKernelOptions;
		
		public KernelOptionsBuilder setUserFunction(IUserFunction pUserFunction)
		{
			mUserFunction = pUserFunction;
			return this;
		}
		
		public KernelOptionsBuilder setTupleDefinitionRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
		{
			mTupleDefinitionsRepository = pTupleDefinitionsRepository;
			return this;
		}
		
		public KernelOptionsBuilder setContextOptions(IOclContextOptions pContextOptions)
		{
			mContextOptions = pContextOptions;
			return this;
		}
		
		public KernelOptionsBuilder setKernelOptions(IOclKernelsOptions pKernelOptions)
		{
			mKernelOptions = pKernelOptions;
			return this;
		}
		
		@Override
		public KernelBuilderOptions build()
		{
			return new KernelBuilderOptions(
				mUserFunction,
				mTupleDefinitionsRepository,
				mContextOptions,
				mKernelOptions
			);
		}
	}
}
