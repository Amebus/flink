package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.common.utility.StringHelper;

import java.io.Serializable;

public interface IUserFunction extends Serializable
{
	
	String getType();
	
	String getName();
	String getFunction();
	String getInputTupleName();
	String getOutputTupleName();
	int getWorkGroupSize();
	
	default boolean isWorkGroupSpecified()
	{
		return getWorkGroupSize() > 0;
	}
	
	default boolean isInputTupleSpecified()
	{
		return !StringHelper.isNullOrWhiteSpace(getInputTupleName());
	}
	
	default boolean isOutputTupleSpecified()
	{
		return !StringHelper.isNullOrWhiteSpace(getOutputTupleName());
	}
	
}
