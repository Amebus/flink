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
		
		public static final String SER_STRING = "#define SER_STRING(s, si, l, r)                \\\n" +
												"        SER_INT(l, si, r);                     \\\n" +
												"        si++;                                  \\\n" +
												"        for(int _ii = 0; _ii < l; _ii++, si++) \\\n" +
												"        {                                      \\\n" +
												"            r[si] = s[_ii];                    \\\n" +
												"        }                                      \\\n";
		
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
												  "            rs = &d[si];            \\\n" +
												  "            si+=ri;                 \\\n";
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
		
		public static final String SER_INT = "SER_INT( " +
											 P1 + ", " +
											 P2 + ", " +
											 RESULT + " );";
		
		public static final String SER_DOUBLE = "SER_DOUBLE( " +
												P1 + ", " +
												P2 + ", " +
												RESULT + ", " +
												LONG_TEMP + " );";
		
		public static final String SER_STRING = "SER_STRING( " +
												P1 + ", " +
												P2 + ", " +
												P3 + ", " +
												RESULT + " );";
	}
	
	public static final class UTILITY_FUNCTIONS
	{
		public static final String INTEGER_TO_STRING = "void integerToString(int n, char *s, int sl)\n" +
													   "{\n" +
													   "    fillStringWith(0, sl, '\\0', s);\n" +
													   "    char const digit[] = \"0123456789\";\n" +
													   "    char* p = s;\n" +
													   "    if(n<0){\n" +
													   "        *p++ = '-';\n" +
													   "        n *= -1;\n" +
													   "    }\n" +
													   "    int shifter = n;\n" +
													   "    do{\n" +
													   "        ++p;\n" +
													   "        shifter = shifter/10;\n" +
													   "    }while(shifter);\n" +
													   "    *p = '\\0';\n" +
													   "    do{\n" +
													   "        *--p = digit[n%10];\n" +
													   "        n = n/10;\n" +
													   "    }while(n);\n" +
													   "}\n";
		
		public static final String STRING_TO_INTEGER = "int stringToInteger(char *s)\n" +
													   "{\n" +
													   "    const char z = '0';\n" +
													   "    int r = 0, st = 0, p = 1;\n" +
													   "    \n" +
													   "    while(s[st] != '\\0')\n" +
													   "    {\n" +
													   "        st++;\n" +
													   "    }\n" +
													   "    for(int i = st-1; i >= 0 && s[i] != '-' ; i--)\n" +
													   "    {\n" +
													   "        r+=((s[i]-z)*p);\n" +
													   "        p*=10;\n" +
													   "    }\n" +
													   "    if(s[0]=='-')\n" +
													   "    {\n" +
													   "        r*=-1;\n" +
													   "    }\n" +
													   "    return r;\n" +
													   "}\n";
		
		public static final String FILL_STRING_WITH = "void fillStringWith(int si,int sl, char c, char *s)\n" +
													  "{\n" +
													  "    for (; si < sl; si++) {\n" +
													  "        s[si] = c;\n" +
													  "    }\n" +
													  "}\n";
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
		pTuple.cIterator()
			  .forEachRemaining(t ->
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
		return getUtilityFunctions() +
			   getSerializationMacros() +
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
	
	public String getUtilityFunctions()
	{
		return UTILITY_FUNCTIONS.FILL_STRING_WITH + "\n" +
			   UTILITY_FUNCTIONS.INTEGER_TO_STRING + "\n" +
			   UTILITY_FUNCTIONS.STRING_TO_INTEGER + "\n";
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
		return MACRO.SER_STRING;
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
			   "\t__global int *" + DATA_INDEXES + ", \n" +
			   "\t__global unsigned char *" + RESULT + ")";
	}
	
	protected String getUtilityVars()
	{
		return "int " + G_ID + " = get_global_id(0);\n" +
			   "unsigned char " + ARITY + " = " + dataOf(0) + ";\n" +
			   "int " + INDEX + " = " + dataIndexOf(G_ID) + ";\n" +
			   "long " + LONG_TEMP + " = 0;" +
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
								 if (x.getCType().isInteger())
								 {
									 vBuilder.append(MACRO_CALL.DESER_INT.replace(MACRO_CALL.P1, x.getName()))
											 .append("\n");
								 }
								 else if (x.getCType().isDouble())
								 {
									 vBuilder.append(MACRO_CALL.DESER_DOUBLE.replace(MACRO_CALL.P1, x.getName()))
											 .append("\n");
								 }
								 else if (x.getCType().isString())
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
