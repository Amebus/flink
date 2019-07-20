package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelSerializationLine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SerializationPlugin extends PDAKernelBuilderPlugin
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
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder
			.append("// serialization")
			.append("\n");
		
		List<KernelSerializationLine> vLines = new ArrayList<>();
		List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
		
		setExtra("ser-" + getIntType(),
				 (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
				 {
					 String vLine = "SER_INT( #, @, _result, _serializationTemp );"
						 .replace("#", pLVar.getVarName())
						 .replace("@","_ri" + pLVar.getIndex());
					 return new KernelSerializationLine(vLine, pLVar.getIndex());
				 })
			.setExtra("ser-" + getDoubleType(),
					  (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
					  {
						  String vLine = "SER_DOUBLE( #, @, _result, _serializationTemp)"
							  .replace("#", pLVar.getVarName())
							  .replace("@","_ri" + pLVar.getIndex());
						  return new KernelSerializationLine(vLine, pLVar.getIndex());
					  })
			.setExtra("ser-" + getStringType(),
					  (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
					  {
						  String vLine = "SER_STRING( #, @, -, _result, _serializationTemp );"
							  .replace("#", pLVar.getVarName())
							  .replace("@","_ri" + pLVar.getIndex())
							  .replace("-", "_rsl" + pLVar.getIndex());
						  return new KernelSerializationLine(vLine, pLVar.getIndex());
					  });
		
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
		
		removeExtra("ser-" + getIntType());
		removeExtra("ser-" + getDoubleType());
		removeExtra("ser-" + getStringType());
	}
}
