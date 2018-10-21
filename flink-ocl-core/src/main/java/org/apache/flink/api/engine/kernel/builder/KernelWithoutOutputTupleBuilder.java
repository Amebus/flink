package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.engine.tuple.variable.OutputVarDefinition;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;

import java.util.ArrayList;

import static org.apache.flink.api.engine.kernel.builder.KernelWithOutputTupleBuilder.OUT_TUPLE_DIM;
import static org.apache.flink.api.engine.kernel.builder.KernelWithOutputTupleBuilder.RESULT_OFFSET;

public abstract class KernelWithoutOutputTupleBuilder extends KernelBuilder
{
	public KernelWithoutOutputTupleBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	private Iterable<VarDefinition> mInputTupleVariablesForResult;
	
	@Override
	protected String getOutputVarDeclaration()
	{
		Iterable<VarDefinition> vDefinitions = getInputTupleVariablesForResult();
		
		return getOutputUtilityVars() +
			   getDeclarationLineForInteger(vDefinitions) +
			   getDeclarationLineForDouble(vDefinitions) +
			   getDeclarationLineForString(vDefinitions) +
			   //getStringResultLengths() +
			   "\n";
	}
	
	@Override
	protected String getOutputSection()
	{
		StringBuilder vBuilder = new StringBuilder();
		
		//getOutputSectionLines().forEach( x -> vBuilder.append(x)
		//											  .append(";\n"));
		
		return vBuilder.toString();
	}
	
//	protected Iterable<String> getOutputSectionLines()
//	{
//		 getInputTupleVariablesAsResult();
//		return new ArrayList<>();
//	}
//
	protected Iterable<VarDefinition> getInputTupleVariablesForResult()
	{
		if (mInputTupleVariablesForResult == null)
		{
			mInputTupleVariablesForResult =
				getTupleVariables(
					getInputTuple(),
					(r, t, i) -> r.add(new OutputVarDefinition(t, i)));
		}
		return  mInputTupleVariablesForResult;
	}
	
	protected String getOutputUtilityVars()
	{
		byte vOffset = 0x1;
		vOffset += getInputTuple().getArity();
		return
			"int " + RESULT_OFFSET + " = " + vOffset + ";\n" +
			"int " + OUT_TUPLE_DIM + " = " + getInputTuple().getMaxDimension() + ";\n";// +
			//getOutputIndexes();
	}
}
