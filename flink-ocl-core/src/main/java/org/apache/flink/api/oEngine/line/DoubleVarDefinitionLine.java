package org.apache.flink.api.oEngine.line;

import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.oEngine.variable.VarDefinition;
import org.apache.flink.api.oEngine.variable.VarDefinitionHelper;
import org.apache.flink.configuration.CTType;

import java.util.stream.Collectors;

public class DoubleVarDefinitionLine extends VarDefinitionKernelLine
{
	public DoubleVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getDoubleVariableNames(pVarDefinitions));
	}
	
	private static Iterable<String> getDoubleVariableNames(Iterable<VarDefinition> pVarDefinitions)
	{
		return VarDefinitionHelper.getDoubleVarDefinitionsAsStream(pVarDefinitions)
							.map(VarDefinition::getName)
							.collect(Collectors.toList());
		
	}
	
	private static String getType(Iterable<VarDefinition> pVarDefinitions)
	{
		String vResult = "";
		if(StreamUtility.streamFrom(pVarDefinitions).anyMatch(x -> x.getCType().isDouble()))
		{
			vResult = CTType.CTypes.DOUBLE;
		}
		return vResult;
	}
}
