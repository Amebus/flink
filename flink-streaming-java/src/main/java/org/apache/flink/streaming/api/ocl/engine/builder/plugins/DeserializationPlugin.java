package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
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
	
	protected KernelDeserializationLine getDeserLine(KernelLogicalVariable pLVar)
	{
		Function<KernelLogicalVariable, KernelDeserializationLine> vF =
			getExtra("deser-" + pLVar.getVarType());
		return vF.apply(pLVar);
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder
			.append("// deserialization")
			.append("\n");
		
		List<KernelDeserializationLine> vLines = new ArrayList<>();
		List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
		
		setExtra("deser-" + getIntLogicalType(),
				 (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
				 {
					 String vLine = "DESER_INT( _data, _i, # );".replace("#", pLVar.getVarName());
					 return new KernelDeserializationLine(vLine, pLVar.getIndex());
				 })
			.setExtra("deser-" + getDoubleLogicalType(),
					  (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
					  {
						  String vLine = "DESER_DOUBLE( _data, _i, # );".replace("#", pLVar.getVarName());
						  return new KernelDeserializationLine(vLine, pLVar.getIndex());
					  })
			.setExtra("deser-" + getStringLogicalType(),
					  (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
					  {
						  String vLine = "DESER_STRING( _data, _i, #, @ );"
							  .replace("#", pLVar.getVarName())
							  .replace("@", "_tsl" + pLVar.getIndex());
						  return new KernelDeserializationLine(vLine, pLVar.getIndex());
					  });
		
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
		
		removeExtra("deser-" + getIntLogicalType());
		removeExtra("deser-" + getDoubleLogicalType());
		removeExtra("deser-" + getStringLogicalType());
	}
}
