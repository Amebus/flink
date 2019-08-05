package org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.InputVarPlugin;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelVariablesLine;

public class LocalBPlugin extends InputVarPlugin
{
	@Override
	public String getVarNamePrefix()
	{
		return "_b";
	}
	
	@Override
	public String getLogicalVarsKey()
	{
		return "input-logical-b-vars";
	}
	
	@Override
	protected String getInputLinesKey()
	{
		return "input-b-lines";
	}
	
	@Override
	protected void setUpExtra()
	{
	
	}
	
	@Override
	protected void setInputLines()
	{
		super.setInputLines();
		getInputLines()[3] = new KernelVariablesLine(getIntLogicalType());
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		pCodeBuilder.append(" local-b\n");
		super.parseTemplateCode(pKernelBuilder, pCodeBuilder);
	}
}
