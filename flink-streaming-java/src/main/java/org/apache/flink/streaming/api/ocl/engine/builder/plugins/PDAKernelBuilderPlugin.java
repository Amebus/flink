package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.FIGenerateExtra;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilderPlugin;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;

import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getStringIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.common.utility.StreamUtility.streamFrom;

public abstract class PDAKernelBuilderPlugin implements IPluginWithExtra
{
	private KernelBuilder mKernelBuilder;
	private StringBuilder mCodeBuilder;
	
	public KernelBuilder getKernelBuilder()
	{
		return mKernelBuilder;
	}
	
	public KernelBuilderOptions getOptions()
	{
		return getKernelBuilder().getKernelBuilderOptions();
	}
	
	public StringBuilder getCodeBuilder()
	{
		return mCodeBuilder;
	}
	
	public <T> T getExtra(String pKey)
	{
		return getKernelBuilder().getExtra(pKey);
	}
	public <T> T getExtra(String pKey, FIGenerateExtra<T> pGenerateExtra)
	{
		if(pGenerateExtra == null)
			throw new IllegalArgumentException(("can't be null"));
		
		T vResult = getExtra(pKey);
		if(vResult == null)
		{
			vResult = pGenerateExtra.generateExtra();
		}
		setExtra(pKey, vResult);
		return vResult;
	}
	public IPluginWithExtra setExtra(String pKey, Object pExtra)
	{
		getKernelBuilder().setExtra(pKey, pExtra);
		return this;
	}
	public <T> T removeExtra(String pKey)
	{
		return getKernelBuilder().removeExtra(pKey);
	}
	
	public PDAKernelBuilderPlugin setKernelBuilder(KernelBuilder pKernelBuilder)
	{
		mKernelBuilder = pKernelBuilder;
		return this;
	}
	
	
	public PDAKernelBuilderPlugin setCodeBuilder(StringBuilder pCodeBuilder)
	{
		mCodeBuilder = pCodeBuilder;
		return this;
	}
	
	public PDAKernelBuilderPlugin setKernelAndCodeBuilder(
		KernelBuilder pKernelBuilder,
		StringBuilder pCodeBuilder)
	{
		return setKernelBuilder(pKernelBuilder)
			.setCodeBuilder(pCodeBuilder);
	}
	
	public static final IKernelBuilderPlugin HELPER_FUNCTIONS =
		(pOptions, pCodeBuilder) ->
			Defaults.getDefaultUtilityFunctions().forEach(p -> pCodeBuilder.append(p).append("\n"));
	
	
	public static final IKernelBuilderPlugin DEFINES =
		(pOptions, pCodeBuilder) ->
		{
			Defaults.getDefaultDeserializationMacrosList().forEach(p -> pCodeBuilder.append(p).append("\n"));
			Defaults.getDefaultSerializationMacrosList().forEach(p -> pCodeBuilder.append(p).append("\n"));
		};
	
	
	public static final IKernelBuilderPlugin KERNEL_ARGS =
		(pOptions, pCodeBuilder) ->
		{
			String vArgs = streamFrom(Defaults.getDefaultKernelParameterList())
				.reduce((a, b) -> a + ",\n" + b).orElse("");
			pCodeBuilder.append(vArgs);
		};
	
	
	public static IKernelBuilderPlugin USER_FUNCTION =
		(pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\n")
				.append("// user function\n")
				.append(pBuilder.getKernelBuilderOptions().getUserFunction().getFunction())
				.append("\n");
	
	public static final class Defaults
	{
		private Defaults() { }
		
		public static Iterable<String> getDefaultUtilityFunctions()
		{
			return getStringIterableFromArgs(Defaults.UtilityFunctions.FILL_STRING_WITH,
											 Defaults.UtilityFunctions.INTEGER_TO_STRING,
											 Defaults.UtilityFunctions.STRING_TO_INTEGER,
											 Defaults.UtilityFunctions.GLOABL_STRING_TO_INTEGER);
		}
		
		public static Iterable<String> getDefaultKernelParameterList()
		{
			return getStringIterableFromArgs("__global unsigned char *_data",
											 "__global int *_dataIndexes",
											 "__global unsigned char *_result");
		}
		
		public static Iterable<String> getDefaultDeserializationMacrosList()
		{
			return getIterableFromArgs(
				Defaults.DeserializationMacros.DESER_INT,
				Defaults.DeserializationMacros.DESER_DOUBLE,
				Defaults.DeserializationMacros.DESER_STRING);
		}
		
		public static Iterable<String> getDefaultSerializationMacrosList()
		{
			return getIterableFromArgs(
				Defaults.SerializationMacros.SER_INT,
				Defaults.SerializationMacros.SER_DOUBLE,
				Defaults.SerializationMacros.SER_STRING);
		}
		
		public static final class VarTypes
		{
			public static final String INT = "int";
			public static final String STRING = "char";
			public static final String DOUBLE = "double";
			public static final String BOOLEAN = "unsigned char";
		}
		
		public static final class LogicalVarTypes
		{
			public static final String INT = "int";
			public static final String STRING = "string";
			public static final String DOUBLE = "double";
			public static final String BOOLEAN = "unsigned char";
		}
		
		public static final class Templates
		{
		}
		
		public static final class FunctionNames
		{
			//Transformations
			public static final String MAP = "map";
			public static final String FILTER = "filter";
			
			//Actions
			public static final String REDUCE = "reduce";
		}
		
		public static final class UtilityFunctions
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
			
			public static final String GLOABL_STRING_TO_INTEGER = "int globalStringToInteger(__global char *s)\n" +
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
		
		public static final class DeserializationMacros
		{
			public static final String DESER_INT = "#define DESER_INT(d, si, r) 				\\\n" +
												   "			r = (*(__global int*) &d[si]);	\\\n" +
												   "			si+=4;							\\\n" +
												   "";
			
			public static final String DESER_DOUBLE = "#define DESER_DOUBLE(d, si, r)  				\\\n" +
													  "			r = (*(__global double*) &d[si]);	\\\n" +
													  "			si+=8;								\\\n" +
													  "";
			
			public static final String DESER_STRING = "#define DESER_STRING(d, si, rs, ri) 			\\\n" +
													  "            DESER_INT(d, si, ri);   			\\\n" +
													  "            rs = (__global char *)&d[si]; 	\\\n" +
													  "            si+=ri;                 			\\\n";
		}
		
		public static final class SerializationMacros
		{
			public static final String SER_INT = "#define SER_INT(i, si, r, t)								\\\n" +
												 "        t = (unsigned char*) &i;							\\\n" +
												 "        for(int _ser_i = 0; _ser_i < 4; _ser_i++, si++)	\\\n" +
												 "        {													\\\n" +
												 "            r[si] = t[_ser_i];							\\\n" +
												 "        }													\\\n" +
												 "";
			
			public static final String SER_DOUBLE = "#define SER_DOUBLE(d, si, r, t)							\\\n" +
													"        t = (unsigned char*) &d;							\\\n" +
													"        for(int _ser_i = 0; _ser_i < 8; _ser_i++, si++)	\\\n" +
													"        {													\\\n" +
													"            r[si] = t[_ser_i];								\\\n" +
													"        }													\\\n" +
													"";
			
			public static final String SER_STRING = "#define SER_STRING(s, si, l, r, t)             \\\n" +
													"        SER_INT(l, si, r, t);					\\\n" +
													"        for(int _ii = 0; _ii < l; _ii++, si++) \\\n" +
													"        {                                      \\\n" +
													"            r[si] = s[_ii];                    \\\n" +
													"        }                                      \\\n";
		}
	}
}