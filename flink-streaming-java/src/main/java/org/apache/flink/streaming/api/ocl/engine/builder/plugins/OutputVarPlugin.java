package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelVariablesLine;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OutputVarPlugin extends PDAKernelBuilderPlugin
{
	protected String getOutputLogicalVarsKey()
	{
		return "output-logical-vars";
	}
	protected String getOutputLinesKey()
	{
		return "output-lines";
	}
	
	protected List<KernelLogicalVariable> getKernelLogicalVariables()
	{
		return getExtra(getOutputLogicalVarsKey(), () ->
		{
			ITupleDefinition vTuple = getOptions().getOutputTuple();
			List<KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = "_r" + vVar.getIndex();
							 String vType = vVar.getType().toLowerCase();
					
							 if(vType.startsWith("i"))
							 {
								 vType = Defaults.VarTypes.INT;
							 }
							 else if(vType.startsWith("d"))
							 {
								 vType = Defaults.VarTypes.DOUBLE;
							 }
							 else if(vType.startsWith("s"))
							 {
								 vType = Defaults.VarTypes.STRING;
							 }
							 vResult.add(
								 new KernelLogicalVariable(vType, vName, vVar.getIndex(), vVar.getMaxReservedBytes()));
						 });
			
			return vResult;
		});
	}
	
	protected KernelVariablesLine[] getOutputLines()
	{
		return getExtra(getOutputLinesKey(),
						() -> new KernelVariablesLine[] {
							new KernelVariablesLine(Defaults.VarTypes.INT),
							new KernelVariablesLine(Defaults.VarTypes.DOUBLE),
							new KernelVariablesLine(Defaults.VarTypes.STRING),
							new KernelVariablesLine(Defaults.VarTypes.INT)
						});
	}
	
	protected void setOutputLines()
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
							 getOutputLines()[3].addVarDef("_rsl" + pVar.getIndex());
							 vVarName += "[" + pVar.getBytesDim() + "]";
						 }
						 getOutputLines()[vIndex].addVarDef(vVarName);
					 });
	}
	
	protected void codeFromOutputLines()
	{
		StringBuilder vCodeBuilder = getCodeBuilder();
		for (KernelVariablesLine vLine : getOutputLines())
		{
			Iterator<String> vIterator = vLine.getVarDefinition().iterator();
			
			if(!vIterator.hasNext())
			{
				continue;
			}
			
			String vType = getExtra("output-var-" + vLine.getVarType());
			
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
			.append("// output-tuple")
			.append("\n");
		
		setOutputLines();
		
		codeFromOutputLines();
	}
}
