package org.apache.flink.api.engine.kernel.line;

import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.engine.VarDefinition;
import org.apache.flink.configuration.CTType;

import java.util.stream.Collectors;

public class StringVarDefinitionLine extends VarDefinitionKernelLine
{
	public StringVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getStringVariableNames(pVarDefinitions));
	}
	
	private static Iterable<String> getStringVariableNames(Iterable<VarDefinition> pVarDefinitions)
	{
		return StreamUtility.streamFrom(pVarDefinitions)
							.filter(x -> x.getCType().isString())
							.map(VarDefinition::getName)
							.collect(Collectors.toList());
	}
	
	private static String getType(Iterable<VarDefinition> pVarDefinitions)
	{
		String vResult = "";
		if(StreamUtility.streamFrom(pVarDefinitions).anyMatch(x -> x.getCType().isString()))
		{
			vResult = CTType.CTypes.STRING;
		}
		return vResult;
	}
}
