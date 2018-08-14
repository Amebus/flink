package org.apache.flink.api.engine.kernel.line;

import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.api.engine.tuple.variable.VarDefinitionHelper;
import org.apache.flink.configuration.CTType;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
