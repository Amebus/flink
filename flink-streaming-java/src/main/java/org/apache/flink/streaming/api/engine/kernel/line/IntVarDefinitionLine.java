package org.apache.flink.streaming.api.engine.kernel.line;

import org.apache.flink.streaming.common.utility.StreamUtility;
import org.apache.flink.streaming.configuration.CTType;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinitionHelper;

import java.util.stream.Collectors;

public class IntVarDefinitionLine extends VarDefinitionKernelLine
{
	public IntVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getIntVariables(pVarDefinitions));
	}
	
	private static String getType(Iterable<VarDefinition> pVarDefinitions)
	{
		String vResult = "";
		if(StreamUtility.streamFrom(pVarDefinitions)
						.anyMatch( x -> x.getCType().isInteger() || x.getCType().isString()))
		{
			vResult = CTType.CTypes.INTEGER;
		}
		return vResult;
	}
	
	private static Iterable<String> getIntVariables(Iterable<VarDefinition> pVarDefinitions)
	{
		return VarDefinitionHelper
			.getIntegerVarDefinitionsAsStream(pVarDefinitions, true)
			.map(VarDefinition::getName)
			.collect(Collectors.toList());
	}
}
