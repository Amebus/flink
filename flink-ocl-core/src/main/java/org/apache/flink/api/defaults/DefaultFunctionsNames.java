package org.apache.flink.api.defaults;

import java.util.ArrayList;
import java.util.List;

public class DefaultFunctionsNames
{
	public static final int SUPPORTED_FUNCTION_TYPES_COUNT = 3;
	
	//Transformations
	public static final String MAP = "map";
	public static final String FILTER = "filter";
	
	//Actions
	public static final String REDUCE = "reduce";
	
	public static Iterable<String> getDefaultFunctionEngineTypes()
	{
		List<String> vFunctionEngineTypes = new ArrayList<>(SUPPORTED_FUNCTION_TYPES_COUNT);
		
		vFunctionEngineTypes.add(FILTER);
		vFunctionEngineTypes.add(MAP);
		vFunctionEngineTypes.add(REDUCE);
		
		return vFunctionEngineTypes;
	}
}
