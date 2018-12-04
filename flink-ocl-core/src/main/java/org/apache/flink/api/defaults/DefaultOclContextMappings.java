package org.apache.flink.api.defaults;

import org.apache.flink.api.engine.IOclContextMappings;
import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;

public class DefaultOclContextMappings implements IOclContextMappings
{
	private FunctionKernelBuilderMapping mFunctionKernelBuilderMapping;
	private StreamWriter mStreamWriter;
	private StreamReader mStreamReader;
	
	
	public DefaultOclContextMappings()
	{
		mFunctionKernelBuilderMapping =
			new DefaultFunctionKernelBuilderMapping(DefaultFunctionsNames.getDefaultFunctionEngineTypes());
		
		mStreamWriter = StreamWriter.getStreamWriter();
		mStreamReader = StreamReader.getStreamReader();
	}
	
	@Override
	public FunctionKernelBuilderMapping getFunctionKernelBuilderMapping()
	{
		return mFunctionKernelBuilderMapping;
	}
	
	@Override
	public StreamWriter getStreamWriter()
	{
		return mStreamWriter;
	}
	
	@Override
	public StreamReader getStreamReader()
	{
		return mStreamReader;
	}
	
	
}
