package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelVariablesLine;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InputVarPlugin extends PDAKernelBuilderPlugin
{
	protected String getInputLogicalVarsKey()
	{
		return "input-logical-vars";
	}
	protected String getInputLinesKey()
	{
		return "input-lines";
	}
	
	protected List<KernelLogicalVariable> getKernelLogicalVariables()
	{
		return getExtra(getInputLogicalVarsKey(), () ->
		{
			ITupleDefinition vTuple = getOptions().getInputTuple();
			List<KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			vTuple.forEach(pVar ->
						   {
							   String vName = "_t" + pVar.getIndex();
							   String vType = pVar.getType().toLowerCase();
				
							   if(vType.startsWith("i"))
							   {
								   vType = getIntType();
							   }
							   else if(vType.startsWith("d"))
							   {
								   vType = getDoubleType();
							   }
							   else if(vType.startsWith("s"))
							   {
								   vType = getStringType();
							   }
				
							   vResult.add(
								   new KernelLogicalVariable(
									   vType,
									   vName,
									   pVar.getIndex(),
									   pVar.getMaxReservedBytes()));
						   });
			return vResult;
		});
	}
	
	protected KernelVariablesLine[] getInputLines()
	{
		return getExtra(getInputLinesKey(),
						() -> new KernelVariablesLine[] {
							new KernelVariablesLine(Defaults.VarTypes.INT),
							new KernelVariablesLine(Defaults.VarTypes.DOUBLE),
							new KernelVariablesLine(Defaults.VarTypes.STRING),
							new KernelVariablesLine(Defaults.VarTypes.INT)
						});
	}
	
	protected void setInputLines()
	{
		getKernelLogicalVariables()
			.forEach(pVar ->
					 {
						 String vVarType = pVar.getVarType();
						 String vVarName = pVar.getVarName();
						 int vIndex = 0;
						 if (vVarType.equals(Defaults.VarTypes.DOUBLE))
						 {
							 vIndex = 1;
						 }
						 else if(vVarType.equals(Defaults.VarTypes.STRING))
						 {
							 vIndex = 2;
							 getInputLines()[3].addVarDef("_tsl" + pVar.getIndex());
						 }
						 getInputLines()[vIndex].addVarDef(vVarName);
					 });
	}
	
	protected void codeFromInputLines()
	{
		StringBuilder vCodeBuilder = getCodeBuilder();
		for (KernelVariablesLine vLine : getInputLines())
		{
			Iterator<String> vIterator = vLine.getVarDefinition().iterator();
			
			if(!vIterator.hasNext())
			{
				continue;
			}
			
			String vType = getExtra("input-var-" + vLine.getVarType());
			
			vCodeBuilder.append(vType)
						.append(" ");
			
			String vVarDef;
			
			while (vIterator.hasNext())
			{
				vVarDef = vIterator.next();
				
				vCodeBuilder.append(vVarDef);
				
				if(vIterator.hasNext())
				{
					vCodeBuilder.append(",");
				}
				else
				{
					vCodeBuilder.append(";\n");
				}
			}
			vCodeBuilder.append("\n");
		}
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder
			.append("\n")
			.append("// input-tuple")
			.append("\n");
		
		setInputLines();
		
		codeFromInputLines();
	}
}
