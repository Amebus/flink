package org.apache.flink.api.engine;

import org.apache.flink.api.common.utility.StringHelper;

import java.io.Serializable;

public interface IUserFunction extends Serializable
{
	
	String getType();
	
	String getName();
	String getFunction();
	String getInputTupleName();
	String getOutputTupleName();
	
	default boolean isInputTupleSpecified()
	{
		return !StringHelper.isNullOrWhiteSpace(getInputTupleName());
	}
	
	default boolean isOutputTupleSpecified()
	{
		return !StringHelper.isNullOrWhiteSpace(getOutputTupleName());
	}
	
}
