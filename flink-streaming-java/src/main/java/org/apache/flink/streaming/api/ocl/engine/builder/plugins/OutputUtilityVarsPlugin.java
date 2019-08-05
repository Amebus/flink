package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import static org.apache.flink.streaming.api.ocl.engine.builder.options.DefaultsValues.DEFAULT_TUPLE_BYTES_DIMENSION_GETTER;

public class OutputUtilityVarsPlugin extends PDAKernelBuilderPlugin
{
	protected ITupleDefinition getOutputTuple()
	{
		return getKernelBuilder().getKernelBuilderOptions().getOutputTuple();
	}
	
	protected byte getOutputOffset()
	{
		byte vOffset = 0x1;
		vOffset += getOutputTuple().getArity();
		return vOffset;
	}
	
	protected int getOutputTupleDimension()
	{
		return DEFAULT_TUPLE_BYTES_DIMENSION_GETTER.getTupleDimension(getOutputTuple());
	}
	
	@Override
	public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
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
