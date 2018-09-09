package org.apache.flink.streaming.api.engine;

import java.io.Serializable;

public interface IUserFunction extends Serializable
{
	String getType();
	
	String getName();
	String getFunction();
	String getInputTupleName();
	String getOutputTupleName();
	
	
	//Transformations
	String MAP = "map";
	String FLAT_MAP = "flatMap";
	String FILTER = "filter";
	
	default boolean isMap()
	{
		return getType().equals(MAP);
	}
	default boolean isFlatMap()
	{
		return getType().equals(FLAT_MAP);
	}
	default boolean isFilter()
	{
		return getType().equals(FILTER);
	}
	
	//Actions
	String REDUCE = "reduce";
	
	default boolean isReduce()
	{
		return getType().equals(REDUCE);
	}
	
	default boolean hasOutputTuple()
	{
		return isMap() || isFlatMap();
	}
	
	default boolean isTransformation()
	{
		return isMap() || isFlatMap() || isFilter();
	}
	
	default boolean isAction()
	{
		return  isReduce();
	}
	
	default boolean isOfKnownType()
	{
		return isTransformation() || isAction();
	}
	
	default boolean isOfUnknownType()
	{
		return !isOfKnownType();
	}
	
	default boolean equals(IUserFunction pOther)
	{
		if(this == pOther)
			return true;
		
		return getType().equals(pOther.getType()) &&
			   getName().equals(pOther.getName()) &&
			   getInputTupleName().equals(pOther.getInputTupleName()) &&
			   getOutputTupleName().equals(pOther.getOutputTupleName()) &&
			   getFunction().equals(pOther.getFunction());
	}
}
