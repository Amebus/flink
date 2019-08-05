package org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.InputVarPlugin;

public class LocalAPlugin extends InputVarPlugin
{
	
	@Override
	public String getVarNamePrefix()
	{
		return "_a";
	}
	
	@Override
	public String getLogicalVarsKey()
	{
		return "input-logical-a-vars";
	}
	
	@Override
	protected String getInputLinesKey()
	{
		return "input-a-lines";
	}
	
	@Override
	protected void setUpExtra()
	{
		this.setExtra("input-var-" + getIntLogicalType(), getIntType())
			.setExtra("input-var-" + getDoubleLogicalType(), getDoubleType())
			.setExtra("input-var-" + getStringLogicalType(), "__local " + getStringType());
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		pCodeBuilder.append(" local-a\n");
		super.parseTemplateCode(pKernelBuilder, pCodeBuilder);
	}
}
