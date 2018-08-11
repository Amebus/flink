package org.apache.flink.api.engine.kernel.builder;

import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.engine.kernel.OclKernel;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.api.engine.kernel.line.DoubleVarDefinitionLine;
import org.apache.flink.api.engine.kernel.line.IntVarDefinitionLine;
import org.apache.flink.api.engine.kernel.line.StringVarDefinitionLine;
import org.apache.flink.api.engine.tuple.variable.InputVarDefinition;
import org.apache.flink.api.engine.tuple.variable.OutputVarDefinition;
import org.apache.flink.api.engine.ITupleVariableAdder;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.configuration.*;

import java.util.ArrayList;
import java.util.Collection;

public abstract class KernelBuilder implements IBuilder<OclKernel>
{
	public static final String G_ID = "_gId";
	public static final String INDEX = "_i";
	
	public static final String RESULT = "_result";
	public static final String K_RESULT = RESULT + "[" + G_ID + "]";
	
	public static final String DATA = "_data";
	public static final String DATA_INDEXES = "_dataIndexes";
	
	public static final String LONG_TEMP = "_l";
	public static final String ARITY = "_arity";
	
	public static final class MACRO
	{
		public static final String SER_INT = "#define SER_INT(i, si, r)          \\\n" +
											 "        r[si] = (i >> 24) & 0xFF;  \\\n" +
											 "        si++;                      \\\n" +
											 "        r[si] = (i >> 16) & 0xFF;  \\\n" +
											 "        si++;                      \\\n" +
											 "        r[si] = (i >> 8) & 0xFF;   \\\n" +
											 "        si++;                      \\\n" +
											 "        r[si] = i & 0xFF;          \\\n";
		
		public static final String SER_DOUBLE = "#define SER_DOUBLE(d, si, r, t)     \\\n" +
												"        t = (long) d;               \\\n" +
												"        r[si] = (t >> 56) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 48) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 40) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 32) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 24) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 16) & 0xFF;   \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = (t >> 8) & 0xFF;    \\\n" +
												"        si++;                       \\\n" +
												"        r[si] = t & 0xFF;           \\\n";
		
		public static final String DESER_INT = "#define DESER_INT(d, si, r) \\\n" +
											   "            r <<= 8;        \\\n" +
											   "            r |= d[si];     \\\n" +
											   "            si++;           \\\n" +
											   "            r <<= 8;        \\\n" +
											   "            r |= d[si];     \\\n" +
											   "            si++;           \\\n" +
											   "            r <<= 8;        \\\n" +
											   "            r |= d[si];     \\\n" +
											   "            si++;           \\\n" +
											   "            r <<= 8;        \\\n" +
											   "            r |= d[si];     \\\n";
		
		public static final String DESER_DOUBLE = "#define DESER_DOUBLE(d, si, r, t)   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            si++;                   \\\n" +
												  "            t <<= 8;                \\\n" +
												  "            t |= d[si];             \\\n" +
												  "            r = (double)t;          \\\n";
		
		public static final String DESER_STRING = "#define DESER_STRING(d, si, rs, ri) \\\n" +
												  "            DESER_INT(d, si, ri);   \\\n" +
												  "            si++;                   \\\n" +
												  "            rs = d[si];             \\\n";
	}
	
	public static final class MACRO_CALL
	{
		public static final String P1 = "$1";
		public static final String P2 = "$2";
		public static final String P3 = "$3";
		
		public static final String DESER_INT = "DESER_INT( " +
											   DATA + ", " +
											   INDEX + ", " +
											   P1 +
											   " );";
		
		public static final String DESER_DOUBLE = "DESER_DOUBLE( " +
												  DATA + ", " +
												  INDEX + ", " +
												  P1 + ", " +
												  LONG_TEMP +
												  " );";
		
		public static final String DESER_STRING = "DESER_STRING( " +
												  DATA + ", " +
												  INDEX + ", " +
												  P1 + ", " +
												  P2 +
												  " );";
	}
	
	private IUserFunction mUserFunction;
	private ITupleDefinitionsRepository mTupleDefinitions;
	private IOclContextOptions mOclContextOptions;
	private IOclKernelsOptions mOclKernelOptions;
	
	public KernelBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		mUserFunction = pKernelBuilderOptions.getUserFunction();
		mTupleDefinitions = pKernelBuilderOptions.getTupleDefinitionsRepository();
		mOclContextOptions = pKernelBuilderOptions.getContextOptions();
		mOclKernelOptions = pKernelBuilderOptions.getKernelOptions();
	}
	
	protected IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	protected IOclContextOptions getOclContextOptions()
	{
		return mOclContextOptions;
	}
	protected IOclKernelsOptions getOclKernelOptions()
	{
		return mOclKernelOptions;
	}
	
	
	protected String dataOf(int pI)
	{
		return dataOf(Integer.toString(pI));
	}
	protected String dataOf(String pS)
	{
		return DATA + "[" + pS + "]";
	}
	
	protected String dataIndexOf(int pI)
	{
		return dataOf(Integer.toString(pI));
	}
	protected String dataIndexOf(String pS)
	{
		return DATA_INDEXES + "[" + pS + "]";
	}
	
	protected ITupleDefinitionsRepository getTupleDefinitions()
	{
		return mTupleDefinitions;
	}
	protected ITupleDefinition getInputTuple()
	{
		return mTupleDefinitions.getTupleDefinition(mUserFunction.getInputTupleName());
	}
	
	protected Iterable<VarDefinition> getInputTupleVariablesForInput()
	{
		return getTupleVariables(getInputTuple(),
								 (r, t, i) -> r.add(new InputVarDefinition(t, i)));
	}
	
	protected Iterable<VarDefinition> getInputTupleVariablesForResult()
	{
		return getTupleVariables(getInputTuple(),
								 (r, t, i) -> r.add(new OutputVarDefinition(t, i)));
	}
	
	protected Iterable<VarDefinition> getTupleVariables(ITupleDefinition pTuple,
														ITupleVariableAdder pResultAdder)
	{
		final int[] vIndex = {0};
		Collection<VarDefinition> vResult = new ArrayList<>(pTuple.getArity());
		pTuple.cIterator().forEachRemaining( t ->
											 {
												 pResultAdder.addTo(vResult, (CTType) t, vIndex[0]);
												 vIndex[0]++;
											 });
		return vResult;
	}
	
	
	protected String getKernelName()
	{
		return getUserFunction().getName();
	}
	
	protected String getKernelCode()
	{
		return getSerializationMacros() +
			   getDeserializationMacros() +
			   getKernelSignature() +
			   "\n{\n" +
			   getUtilityVars() + "\n" +
			   getOutputVarDeclaration() + "\n" +
			   getInputSection() + "\n" +
			   getUserFunction().getFunction() + "\n" +
			   getOutputSection() +
			   "\n};\n";
	}
	
	@Override
	public OclKernel build()
	{
		return new OclKernel(getKernelName(), getKernelCode());
	}
	
	protected String getDeserializationMacroForInt()
	{
		return MACRO.DESER_INT;
	}
	protected String getDeserializationMacroForDouble()
	{
		return MACRO.DESER_DOUBLE;
	}
	protected String getDeserializationMacroForString()
	{
		return MACRO.DESER_STRING;
	}
	
	protected String getSerializationMacroForInt()
	{
		return MACRO.SER_INT;
	}
	protected String getSerializationMacroForDouble()
	{
		return MACRO.SER_DOUBLE;
	}
	protected String getSerializationMacroForString()
	{
		return "";
	}
	
	protected String getSerializationMacros()
	{
		return getSerializationMacroForInt() + "\n" +
			   getSerializationMacroForDouble() + "\n" +
			   getSerializationMacroForString() + "\n";
	}
	
	protected String getDeserializationMacros()
	{
		return getDeserializationMacroForInt() + "\n" +
			   getDeserializationMacroForDouble() + "\n" +
			   getDeserializationMacroForString() + "\n";
	}
	
	protected String getKernelSignature()
	{
		return "__kernel void " +
			   getKernelName() +
			   "(\n" +
			   "\t__global unsigned char *" + DATA + ", \n" +
			   "\t__global int *" + DATA_INDEXES +", \n" +
			   "\t__global unsigned char *" + RESULT + ")";
	}
	
	protected String getUtilityVars()
	{
		return "int " + G_ID + " = get_global_id(0);\n" +
			   "unsigned char " + ARITY + " = " + dataOf(0) + ";\n" +
			   "int " + INDEX + " = " + dataIndexOf(G_ID) + ";\n" +
			   "long " + LONG_TEMP + " = 0;"+
			   "\n";
	}
	
	protected String getInputSection()
	{
		return getInputDeclarationVariables() +
			   getInputDeserialization();
	}
	
	protected String getInputDeclarationVariables()
	{
		Iterable<VarDefinition> vDefinitions = getInputTupleVariablesForInput();
		
		return getDeclarationLineForInteger(vDefinitions) +
			   getDeclarationLineForDouble(vDefinitions) +
			   getDeclarationLineForString(vDefinitions) +
			   "\n";
	}
	
	protected String getDeclarationLineForInteger(Iterable<VarDefinition> pDefinitions)
	{
		return new IntVarDefinitionLine(pDefinitions).toKernelLine();
	}
	protected String getDeclarationLineForDouble(Iterable<VarDefinition> pDefinitions)
	{
		return new DoubleVarDefinitionLine(pDefinitions).toKernelLine();
	}
	protected String getDeclarationLineForString(Iterable<VarDefinition> pDefinitions)
	{
		return new StringVarDefinitionLine(pDefinitions).toKernelLine();
	}
	
	protected String getInputDeserialization()
	{
		StringBuilder vBuilder = new StringBuilder();
		Iterable<VarDefinition> vDefinitions = getInputTupleVariablesForInput();
		
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
								 else if(x.getCType().isString())
								 {
									 String vDimName = "_sl" + x.getIndex();
									 vBuilder.append(MACRO_CALL.DESER_STRING
														 .replace(MACRO_CALL.P1, x.getName())
														 .replace(MACRO_CALL.P2, vDimName)
													)
											 .append("\n");
				
								 }
							 });
		return vBuilder.toString();
	}
	
	protected abstract String getOutputVarDeclaration();
	protected abstract String getOutputSection();
}
