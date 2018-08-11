package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.engine.tuple.variable.VarDefinition;

public class MapBuilder extends KernelWithOutputTupleBuilder
{
	
	public MapBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	@Override
	protected String getOutputSection()
	{
		StringBuilder vBuilder = new StringBuilder();
		Iterable<VarDefinition> vDefinitions = getOutputTupleVariablesAsResult();
		
		vDefinitions.forEach(x ->
							 {
								 if(x.getCType().isInteger())
								 {
									 vBuilder.append(MACRO_CALL.DESER_INT.replace(MACRO_CALL.P1, x.getName()))
											 .append("\n");
								 }
								 else if(x.getCType().isDouble())
								 {
									 vBuilder.append(MACRO_CALL.DESER_DOUBLE.replace(MACRO_CALL.P1, x.getName()))
											 .append("\n");
								 }
							 });
		return vBuilder.toString();
	}
	
}
