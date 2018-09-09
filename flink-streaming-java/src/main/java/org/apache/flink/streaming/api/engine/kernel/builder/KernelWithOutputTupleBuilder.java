package org.apache.flink.streaming.api.engine.kernel.builder;

import org.apache.flink.streaming.api.engine.tuple.variable.OutputVarDefinition;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.streaming.api.engine.tuple.variable.VarDefinitionHelper;
import org.apache.flink.streaming.configuration.CTType;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.Iterator;

public abstract class KernelWithOutputTupleBuilder extends KernelBuilder
{
	/**
	 * Represent the offset to apply to the index of the result stream for every tuple
	 */
	public final static String RESULT_OFFSET = "_roff";
	public final static String OUT_TUPLE_DIM = "_otd";
	public final static String RESULT_INDEX = "_ri";
	
	private Iterable<VarDefinition> mOutputTupleVariablesAsResult;
	
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
		if(mOutputTupleVariablesAsResult == null)
		{
			mOutputTupleVariablesAsResult =
				getTupleVariables(
					getOutputTuple(),
					(r, t, i) -> r.add(new OutputVarDefinition(t, i)));
		}
		return mOutputTupleVariablesAsResult;
	}
	
	@Override
	protected String getOutputVarDeclaration()
	{
		Iterable<VarDefinition> vDefinitions = getOutputTupleVariablesAsResult();
		
		return getOutputUtilityVars() +
			   getDeclarationLineForInteger(vDefinitions) +
			   getDeclarationLineForDouble(vDefinitions) +
			   getDeclarationLineForString(vDefinitions) +
			   getStringResultLengths() +
			   "\n";
	}
	
	protected String getOutputUtilityVars()
	{
		byte vOffset = 0x1;
		vOffset += getOutputTuple().getArity();
		return
			"int " + RESULT_OFFSET + " = " + vOffset + ";\n" +
			"int " + OUT_TUPLE_DIM + " = " + getOutputTuple().getMaxDimension() + ";\n" +
			getOutputIndexes();
	}
	
	protected String getOutputIndexes()
	{
		return getFirstResultIndex() +
			   getTailResultIndexes() +
			   "\n";
	}
	
	private String getTailResultIndexes()
	{
		StringBuilder vBuilder = new StringBuilder();
		Iterable<VarDefinition> vIterable = getOutputTupleVariablesAsResult();
		Iterator<VarDefinition> vCurrentDefinitionIterator = vIterable.iterator();
		Iterator<VarDefinition> vPreviousDefinitionIterator = vIterable.iterator();
		
		vCurrentDefinitionIterator.next();
		vCurrentDefinitionIterator
			.forEachRemaining(vd -> vBuilder.append(getTailResultIndex(vd, vPreviousDefinitionIterator.next())));
		return vBuilder.toString();
	}
	
	private String getTailResultIndex(VarDefinition pCurrentDefinition, VarDefinition pPreviousDefinition)
	{
		CTType vPreviousType = pPreviousDefinition.getCType();
		int vPreviousTypeDim = vPreviousType.getMaxByteOccupation();
		if (vPreviousType.isString())
		{
			vPreviousTypeDim += 4;
		}
		return "int " +
			   getResultIndex(pCurrentDefinition.getIndex()) +
			   " = " +
			   getResultIndex(pPreviousDefinition.getIndex()) +
			   " + " +
			   vPreviousTypeDim +
			   ";\n";
	}
	
	private String getFirstResultIndex()
	{
		return "int " +
			   getResultIndex(0) +
			   " = " +
			   RESULT_OFFSET +
			   " + " +
			   G_ID +
			   " * " +
			   OUT_TUPLE_DIM +
			   ";\n";
	}
	
	protected String getResultIndex(int pI)
	{
		return RESULT_INDEX + pI;
	}
	
	protected String getStringResultLengths()
	{
		StringBuilder vBuilder = new StringBuilder();
		
		VarDefinitionHelper
			.getStringLengthVarDefinitions(getOutputTupleVariablesAsResult())
			.forEach(x ->	vBuilder.append(x.getName())
									 .append(" = ")
									 .append(x.getLength())
									 .append(";\n"));
		
		return vBuilder.toString();
	}
	
	@Override
	protected String getOutputSection()
	{
		StringBuilder vBuilder = new StringBuilder();
		
		getOutputTupleVariablesAsResult()
			.forEach(pVD ->
					 {
						 if(pVD.getCType().isInteger())
						 {
							 vBuilder.append(MACRO_CALL.SER_INT
												 .replace(MACRO_CALL.P1, pVD.getName())
												 .replace(MACRO_CALL.P2, getResultIndex(pVD.getIndex())));
						 }
						 else if(pVD.getCType().isDouble())
						 {
							 vBuilder.append(MACRO_CALL.SER_DOUBLE
												 .replace(MACRO_CALL.P1, pVD.getName())
												 .replace(MACRO_CALL.P2, getResultIndex(pVD.getIndex())));
						 }
						 else if(pVD.getCType().isString())
						 {
						 	vBuilder.append(MACRO_CALL.SER_STRING
												.replace(MACRO_CALL.P1, pVD.getName())
												.replace(MACRO_CALL.P2, getResultIndex(pVD.getIndex()))
												.replace(MACRO_CALL.P3, String.valueOf(pVD.getLength())));
						 }
						 
						 if(pVD.getCType().isKnown())
						 {
						 	vBuilder.append("\n");
						 }
					 });
		return vBuilder.toString();
	}
}
