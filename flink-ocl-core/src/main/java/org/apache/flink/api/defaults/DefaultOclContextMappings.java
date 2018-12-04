package org.apache.flink.api.defaults;

import org.apache.flink.api.engine.IOclContextMappings;
import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;

public class DefaultOclContextMappings implements IOclContextMappings
{
	private FunctionKernelBuilderMapping mFunctionKernelBuilderMapping;
	
	public DefaultOclContextMappings()
	{
		mFunctionKernelBuilderMapping =
			new DefaultFunctionKernelBuilderMapping(DefaultFunctionsNames.getDefaultFunctionEngineTypes());
	}
	
	@Override
	public FunctionKernelBuilderMapping getFunctionKernelBuilderMapping()
	{
		return mFunctionKernelBuilderMapping;
	}
}
