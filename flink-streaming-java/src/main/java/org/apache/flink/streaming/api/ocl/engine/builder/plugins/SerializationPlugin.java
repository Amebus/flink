package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelSerializationLine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SerializationPlugin extends PDAKernelBuilderPlugin implements IPluginWithVarTypes
{
	protected String getOutputLogicalVarsKey()
	{
		return "output-logical-vars";
	}
	
	protected List<KernelLogicalVariable> getKernelLogicalVariables()
	{
		return getExtra(getOutputLogicalVarsKey(), () ->
		{
			throw new IllegalStateException(
				"the \"" +
				getOutputLogicalVarsKey() +
				"\" Extra must be defined before the usage of the \"Serialization\" plugin.");
		});
	}
	
	protected KernelSerializationLine getSerLine(KernelLogicalVariable pLVar)
	{
		Function<KernelLogicalVariable, KernelSerializationLine> vF =
			getExtra("ser-" + pLVar.getVarType());
		return vF.apply(pLVar);
	}
	
	protected String getResultVariableName()
	{
		return "_result";
	}
	protected String getIndexVariableName()
	{
		return "_ri";
	}
	protected String getStringLengthVarPrefix()
	{
		return "_rsl";
	}
	
	protected void setUpExtras()
	{
		setExtra("ser-" + getIntLogicalType(),
				 (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
				 {
					 String vLine = "SER_INT( #1, #2, #3, _serializationTemp );"
						 .replace("#1", pLVar.getVarName())
						 .replace("#2", getIndexVariableName())
						 .replace("#3", getResultVariableName());
					 return new KernelSerializationLine(vLine, pLVar.getIndex());
				 })
			.setExtra("ser-" + getDoubleLogicalType(),
					  (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
					  {
						  String vLine = "SER_DOUBLE( #1, #2, #3, _serializationTemp)"
							  .replace("#1", pLVar.getVarName())
							  .replace("#2", getIndexVariableName())
							  .replace("#3", getResultVariableName());
						  return new KernelSerializationLine(vLine, pLVar.getIndex());
					  })
			.setExtra("ser-" + getStringLogicalType(),
					  (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
					  {
						  String vLine = "SER_STRING( #1,#2, #3, #4, _serializationTemp );"
							  .replace("#1", pLVar.getVarName())
							  .replace("#2",getIndexVariableName())
							  .replace("#3", getStringLengthVarPrefix() + pLVar.getIndex())
							  .replace("#4", getResultVariableName());
						  return new KernelSerializationLine(vLine, pLVar.getIndex());
					  });
	}
	
	protected void removeExtras()
	{
		removeExtra("ser-" + getIntType());
		removeExtra("ser-" + getDoubleType());
		removeExtra("ser-" + getStringType());
	}
	
	@Override
	public void parseTemplateCode(KernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder
			.append("// serialization")
			.append("\n");
		
		List<KernelSerializationLine> vLines = new ArrayList<>();
		List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
		
		setUpExtras();
		
		vLogicalVariables.forEach( pLVar -> vLines.add(getSerLine(pLVar)) );
		
		vLines.sort((o1, o2) ->
					{
						int vResult = 0;
						if(o1.getSerIndexOrder() > o2.getSerIndexOrder())
						{
							vResult = 1;
						}
						else if(o1.getSerIndexOrder() < o2.getSerIndexOrder())
						{
							vResult = -1;
						}
						return vResult;
					});
		
		vLines.forEach(pLine -> pCodeBuilder.append(pLine.getSerLine()).append("\n"));
		pCodeBuilder.append("\n");
		
		removeExtras();
	}
}
