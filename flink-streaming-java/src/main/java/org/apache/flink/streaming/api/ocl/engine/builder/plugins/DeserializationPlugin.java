package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelDeserializationLine;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DeserializationPlugin extends PDAKernelBuilderPlugin implements IPluginWithVarTypes
{
	protected String getInputLogicalVarsKey()
	{
		return "input-logical-vars";
	}
	
	protected List<KernelLogicalVariable> getKernelLogicalVariables()
	{
		return getExtra(getInputLogicalVarsKey(), () ->
		{
			throw new IllegalStateException(
				"the \"" +
				getInputLogicalVarsKey() +
				"\" Extra must be defined before the usage of the \"Deserialization\" plugin.");
		});
	}
	
	protected String getDeserExtraKey(KernelLogicalVariable pLVar)
	{
		return getDeserExtraKey(pLVar.getVarType());
	}
	protected String getDeserExtraKey(String pLogicalType)
	{
		return "deser-" + pLogicalType;
	}
	
	
	protected KernelDeserializationLine getDeserLine(KernelLogicalVariable pLVar)
	{
		Function<KernelLogicalVariable, KernelDeserializationLine> vF =
			getExtra(getDeserExtraKey(pLVar));
		return vF.apply(pLVar);
	}
	
	protected String getDataVariableName()
	{
		return "_data";
	}
	protected String getIndexVariableNAme()
	{
		return "_i";
	}
	protected String getStringLengthVarPrefix()
	{
		return "_tsl";
	}
	
	protected void setUpExtras()
	{
		setExtra(getDeserExtraKey(getIntLogicalType()),
				 (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
				 {
					 String vLine = "DESER_INT( #1, #2, #3 );"
						 .replace("#1", getDataVariableName())
						 .replace("#2", getIndexVariableNAme())
						 .replace("#3", pLVar.getVarName());
					 return new KernelDeserializationLine(vLine, pLVar.getIndex());
				 })
			.setExtra(getDeserExtraKey(getDoubleLogicalType()),
					  (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
					  {
						  String vLine = "DESER_DOUBLE( #1, #2, #3 );"
							  .replace("#1", getDataVariableName())
							  .replace("#2", getIndexVariableNAme())
							  .replace("#3", pLVar.getVarName());
						  return new KernelDeserializationLine(vLine, pLVar.getIndex());
					  })
			.setExtra(getDeserExtraKey(getStringLogicalType()),
					  (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
					  {
						  String vLine = "DESER_STRING( #1, #2, #3, #4 );"
							  .replace("#1", getDataVariableName())
							  .replace("#2", getIndexVariableNAme())
							  .replace("#3", pLVar.getVarName())
							  .replace("#4", getStringLengthVarPrefix() + pLVar.getIndex());
						  return new KernelDeserializationLine(vLine, pLVar.getIndex());
					  });
	}
	
	protected void removeExtras()
	{
		removeExtra(getDeserExtraKey(getDoubleLogicalType()));
		removeExtra(getDeserExtraKey(getDoubleLogicalType()));
		removeExtra(getDeserExtraKey(getStringLogicalType()));
	}
	
	@Override
	public void parseTemplateCode(KernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder.append("// deserialization\n");
		
		List<KernelDeserializationLine> vLines = new ArrayList<>();
		List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
		
		setUpExtras();
		
		vLogicalVariables.forEach( pLVar -> vLines.add(getDeserLine(pLVar)) );
		
		vLines.sort((o1, o2) ->
					{
						int vResult = 0;
						if(o1.getDeserIndexOrder() > o2.getDeserIndexOrder())
						{
							vResult = 1;
						}
						else if(o1.getDeserIndexOrder() < o2.getDeserIndexOrder())
						{
							vResult = -1;
						}
						return vResult;
					});
		
		vLines.forEach(pLine -> pCodeBuilder.append(pLine.getDeserLine()).append("\n"));
		pCodeBuilder.append("\n");
		
		removeExtras();
	}
}
