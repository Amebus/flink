package org.apache.flink.api.engine.kernel.line;

import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.common.utility.StringHelper;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.api.engine.tuple.variable.VarDefinitionHelper;
import org.apache.flink.configuration.CTType;

import java.util.stream.Collectors;

public class StringVarDefinitionLine extends VarDefinitionKernelLine
{
	private boolean isOutputLineDefinition;
	
	public StringVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getStringVariableNames(pVarDefinitions));
		
		isOutputLineDefinition = VarDefinitionHelper
			.getStringVarDefinitionsAsStream(pVarDefinitions)
			.anyMatch(VarDefinition::isOutputVar);
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
			vResult = "unsigned " + CTType.CTypes.STRING;
			if (StreamUtility.streamFrom(pVarDefinitions).anyMatch(VarDefinition::isOutputVar))
			{
				vResult = vResult.substring(0, vResult.length()-1);
			}
		}
		return vResult;
	}
	
	@Override
	public String toKernelLine()
	{
		String vResult = super.toKernelLine();
		
		if(!isOutputLineDefinition && !StringHelper.isNullOrWhiteSpace(vResult))
			vResult = "__global " + vResult;
		
		return vResult;
	}
}
