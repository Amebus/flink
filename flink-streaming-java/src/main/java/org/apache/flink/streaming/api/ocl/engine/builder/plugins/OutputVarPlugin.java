package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelVariablesLine;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.Iterator;

public class OutputVarPlugin extends PDAKernelBuilderPlugin implements IPluginWithLogicalVariables
{
	@Override
	public String getLogicalVarsKey()
	{
		return "output-logical-vars";
	}
	protected String getOutputLinesKey()
	{
		return "output-lines";
	}
	
	@Override
	public ITupleDefinition getTuple()
	{
		return getOptions().getOutputTuple();
	}
	
	@Override
	public String getVarNamePrefix()
	{
		return "_r";
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
							 getOutputLines()[3].addVarDef("_rsl" + pVar.getIndex() + " = " + pVar.getBytesDim());
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
