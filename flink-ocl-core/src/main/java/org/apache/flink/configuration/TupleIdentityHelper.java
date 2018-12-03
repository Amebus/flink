package org.apache.flink.configuration;

public final class TupleIdentityHelper
{
	public static Integer getIntegerValue(TupleVarDefinition pTupleVarDefinition)
	{
		TType vType = pTupleVarDefinition.getJavaT();
		
		if(vType.isInteger()) {
			return (Integer)pTupleVarDefinition.getIdentityValue();
		}
		
		throw new IllegalArgumentException("Unable to cast " + vType + " value to \"Integer\"");
	}
	
	public static Double getDoubleValue(TupleVarDefinition pTupleVarDefinition)
	{
		TType vType = pTupleVarDefinition.getJavaT();
		
		if(vType.isDouble()) {
			return (Double) pTupleVarDefinition.getIdentityValue();
		}
		
		throw new IllegalArgumentException("Unable to cast " + vType + " value to \"Double\"");
	}
	
	public static String getStringValue(TupleVarDefinition pTupleVarDefinition)
	{
		TType vType = pTupleVarDefinition.getJavaT();
		
		if(vType.isString()) {
			return (String) pTupleVarDefinition.getIdentityValue();
		}
		
		throw new IllegalArgumentException("Unable to cast " + vType + " value to \"String\"");
	}
}
