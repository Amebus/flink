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
								 vType = getIntLogicalType();
							 }
							 else if(vType.startsWith("d"))
							 {
								 vType = getDoubleLogicalType();
							 }
							 else if(vType.startsWith("s"))
							 {
								 vType = getStringLogicalType();
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
							new KernelVariablesLine(getIntLogicalType()),
							new KernelVariablesLine(getDoubleLogicalType()),
							new KernelVariablesLine(getStringLogicalType()),
							new KernelVariablesLine(getIntLogicalType())
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
						 if (vVarType.equals(getDoubleLogicalType()))
						 {
							 vIndex = 1;
						 }
						 else if(vVarType.equals(getStringLogicalType()))
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
		}
		vCodeBuilder.append("\n");
	}
	
	protected void setUpExtra()
	{
		this.setExtra("output-var-" + getIntLogicalType(), getIntType())
			.setExtra("output-var-" + getDoubleLogicalType(), getDoubleType())
			.setExtra("output-var-" + getStringLogicalType(), getStringType());
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		setUpExtra();
		
		pCodeBuilder
			.append("\n")
			.append("// output-tuple")
			.append("\n");
		
		setOutputLines();
		
		codeFromOutputLines();
	}
}
