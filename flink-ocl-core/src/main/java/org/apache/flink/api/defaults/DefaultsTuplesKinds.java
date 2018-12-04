package org.apache.flink.api.defaults;

import java.util.ArrayList;
import java.util.List;

public class DefaultsTuplesKinds
{
	public static final int SUPPORTED_TUPLE_KINDS_COUNT = 2;
	
	public static final String INPUT_TUPLE = "input-tuple";
	public static final String OUTPUT_TUPLE = "output-tuple";
	
	public static final String CACHE_TUPLE = "cache-tuple";
	
	public static Iterable<String> getDefaultTuplesEngineTypes()
	{
		List<String> vFunctionEngineTypes = new ArrayList<>(SUPPORTED_TUPLE_KINDS_COUNT);
		
		vFunctionEngineTypes.add(INPUT_TUPLE);
		vFunctionEngineTypes.add(OUTPUT_TUPLE);
		
		return vFunctionEngineTypes;
	}
}
