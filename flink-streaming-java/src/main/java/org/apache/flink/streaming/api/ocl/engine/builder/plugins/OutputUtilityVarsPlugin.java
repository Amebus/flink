package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.configuration.ITupleDefinition;

public class OutputUtilityVarsPlugin extends PDAKernelBuilderPlugin
{
	protected ITupleDefinition getOutputTuple()
	{
		return getOptions().getOutputTuple();
	}
	
	protected byte getOutputOffset()
	{
		byte vOffset = 0x1;
		vOffset += getOutputTuple().getArity();
		return vOffset;
	}
	
	protected int getOutputTupleDimension()
	{
		return getOptions().getTupleBytesDimensionGetter().getTupleDimension(getOutputTuple());
	}
	
	@Override
	public void parseTemplateCode(KernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
	{
		setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
		
		pCodeBuilder
			.append("\n")
			.append("// output utility vars")
			.append("\n")
			.append("uint _roff = ")
			.append(getOutputOffset())
			.append(";\n")
			.append("uint _otd = ")
			.append(getOutputTupleDimension())
			.append(";\n")
			.append("uint _ri = _roff + _gId * _otd;\n");
	}
}
