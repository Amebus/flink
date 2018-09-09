package org.apache.flink.streaming.api.engine.kernel.line;

import org.apache.flink.streaming.common.utility.StreamUtility;
import org.apache.flink.streaming.configuration.CTType;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinitionHelper;

import java.util.stream.Collectors;

public class StringVarDefinitionLine extends VarDefinitionKernelLine
{
	public StringVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getStringVariableNames(pVarDefinitions));
	}
	
	private static Iterable<String> getStringVariableNames(Iterable<VarDefinition> pVarDefinitions)
	{
		return VarDefinitionHelper.getStringVarDefinitionsAsStream(pVarDefinitions)
								  .map(VarDefinition::getName)
								  .collect(Collectors.toList());
	}
	
	private static String getType(Iterable<VarDefinition> pVarDefinitions)
	{
		String vResult = "";
		
		if(StreamUtility.streamFrom(pVarDefinitions).anyMatch(x -> x.getCType().isString()))
		{
			vResult = CTType.CTypes.STRING;
			if (StreamUtility.streamFrom(pVarDefinitions).anyMatch(VarDefinition::isOutputVar))
			{
				vResult = vResult.substring(0, vResult.length()-1);
			}
		}
		return vResult;
	}
}
