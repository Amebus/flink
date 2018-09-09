package org.apache.flink.streaming.api.engine.kernel.builder;

import org.apache.flink.streaming.common.IBuilder;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.ITupleDefinitionsRepository;
import org.apache.flink.streaming.api.engine.IUserFunction;

public class KernelBuilderOptions
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionsRepository mTupleDefinitionsRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	public KernelBuilderOptions(
		IUserFunction pUserFunction,
		ITupleDefinitionsRepository pTupleDefinitionsRepository,
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
	
	public ITupleDefinitionsRepository getTupleDefinitionsRepository()
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
		private ITupleDefinitionsRepository mTupleDefinitionsRepository;
		private IOclContextOptions mContextOptions;
		private IOclKernelsOptions mKernelOptions;
		
		public KernelOptionsBuilder setUserFunction(IUserFunction pUserFunction)
		{
			mUserFunction = pUserFunction;
			return this;
		}
		
		public KernelOptionsBuilder setTupleDefinitionsRepository(ITupleDefinitionsRepository pTupleDefinitionsRepository)
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
