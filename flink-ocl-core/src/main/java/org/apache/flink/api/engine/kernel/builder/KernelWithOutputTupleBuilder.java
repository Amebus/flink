package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.engine.tuple.variable.OutputVarDefinition;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.configuration.ITupleDefinition;

import java.util.ArrayList;

public abstract class KernelWithOutputTupleBuilder extends KernelBuilder
{
	
	public KernelWithOutputTupleBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	protected ITupleDefinition getOutputTuple()
	{
		return getTupleDefinitions().getTupleDefinition(getUserFunction().getOutputTupleName());
	}
	
	protected Iterable<VarDefinition> getOutputTupleVariablesAsResult()
	{
		return getTupleVariables(getOutputTuple(),
								 (r, t, i) -> r.add(new OutputVarDefinition(t, i)));
//		return new ArrayList<>();
	}
	
	@Override
	protected String getOutputVarDeclaration()
	{
		Iterable<VarDefinition> vDefinitions = getOutputTupleVariablesAsResult();
		
		return getDeclarationLineForInteger(vDefinitions) +
			   getDeclarationLineForDouble(vDefinitions) +
			   getDeclarationLineForString(vDefinitions) +
			   "\n";
	}
}
