package org.apache.flink.api.newEngine.kernel.builder.options;

import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.engine.IOclContextMappings;
import org.apache.flink.api.engine.kernel.builder.FilterBuilder;
import org.apache.flink.api.engine.kernel.builder.MapBuilder;
import org.apache.flink.api.engine.kernel.builder.ReduceBuilder;
import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilder;
import org.apache.flink.api.newEngine.kernel.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.TupleKindVarTypeToKernelTypeMapper;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;

import static org.apache.flink.api.common.utility.IterableHelper.getStringIterableFromArgs;

public class DefaultsValues
{
	public static Iterable<String> getDefaultKernelParameterList()
	{
		return getStringIterableFromArgs("__global unsigned char *_data",
										 "__global int *_dataIndexes",
										 "__global unsigned char *_result");
	}
	
	public static Iterable<String> getDefaultFunctionEngineTypes()
	{
		return getStringIterableFromArgs(DefaultFunctionsNames.FILTER,
										 DefaultFunctionsNames.MAP,
										 DefaultFunctionsNames.REDUCE);
	}
	
	public static Iterable<String> getDefaultUtilityFunctions()
	{
		return getStringIterableFromArgs(DefaultUtilityFunctions.FILL_STRING_WITH,
										 DefaultUtilityFunctions.INTEGER_TO_STRING,
										 DefaultUtilityFunctions.STRING_TO_INTEGER,
										 DefaultUtilityFunctions.GLOABL_STRING_TO_INTEGER);
	}
	
	public static Iterable<String> getDefaultTuplesEngineKinds()
	{
		return getStringIterableFromArgs(DefaultsTuplesKinds.INPUT_TUPLE,
										 DefaultsTuplesKinds.OUTPUT_TUPLE);
	}
	
	public static Iterable<String> getDefaultVarTypes()
	{
		return getStringIterableFromArgs(DefaultVarTypes.INT,
										 DefaultVarTypes.DOUBLE,
										 DefaultVarTypes.STRING);
	}
	
	public static KernelBuilder.IUtilityVariablesGetter getDefaultUtilityVariableGetter()
	{
		return new DefaultUtilityVariableGetter();
	}
	
	public static KernelBuilder.ITupleKindVariableTypeKeyCalculator getTupleKindVariableTypeKeyCalculator()
	{
		return (pTupleKinds, pVarType) -> pTupleKinds + "-" + pVarType;
	}
	
	public static FunctionKernelBuilderMapper getDefaultFunctionKernelBuilderMapping()
	{
		return new DefaultFunctionKernelBuilderMapping();
	}
	
	public static DefaultOclContextMappings getDefaultOclContextMappings()
	{
		return new DefaultOclContextMappings();
	}
	
	public static class Map
	{
		public static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper(
			KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
		{
			TupleKindVarTypeToKernelTypeMapper vMapper = DefaultsValues.getTupleKindVarTypeToKernelTypeMapper();
			
			DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithInput(vMapper, pKeyCalculator);
			DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithOutput(vMapper, pKeyCalculator);
			
			return vMapper;
		}
	}
	
	public static class Filter
	{
		public static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper(
			KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
		{
			TupleKindVarTypeToKernelTypeMapper vMapper = DefaultsValues.getTupleKindVarTypeToKernelTypeMapper();
			
			DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithInput(vMapper, pKeyCalculator);
			//DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithOutput(vMapper, pKeyCalculator);
			
			return vMapper;
		}
	}
	
	public static class Reduce
	{
	
	}
	
	public static class DefaultUtilityFunctions
	{
		public static final int UTILITY_FUNCTION_COUNT = 4;
		
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
	
	public static class DefaultsTuplesKinds
	{
		public static final int SUPPORTED_TUPLE_KINDS_COUNT = 2;
		
		public static final String INPUT_TUPLE = "input-tuple";
		public static final String OUTPUT_TUPLE = "output-tuple";
		
		public static final String CACHE_TUPLE = "cache-tuple";
	}
	
	public static class DefaultVarTypes
	{
		public static final int SUPPORTED_TUPLE_VAR_TYPES_COUNT = 3;
		
		public static final String INT = "int";
		public static final String DOUBLE = "double";
		public static final String STRING = "string";
	}
	
	public static class DefaultsSerializationTypes
	{
		public static final int SUPPORTED_SERIALIZATION_TYPES_COUNT = 3;
		
		public static final byte INT = 1;
		public static final byte DOUBLE = 2;
		public static final byte STRING = 3;
	}
	
	public static class DefaultFunctionsNames
	{
		public static final int SUPPORTED_FUNCTION_TYPES_COUNT = 3;
		
		//Transformations
		public static final String MAP = "map";
		public static final String FILTER = "filter";
		
		//Actions
		public static final String REDUCE = "reduce";
	}
	
	public static class DefaultFunctionKernelBuilderMapping extends FunctionKernelBuilderMapper
	{
		public DefaultFunctionKernelBuilderMapping()
		{
			super(new StringKeyCaseInsenstiveComparer(""));
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(DefaultFunctionsNames.MAP, KernelBuilder::new);
			register(DefaultFunctionsNames.FILTER, KernelBuilder::new);
			register(DefaultFunctionsNames.REDUCE, KernelBuilder::new);
		}
	}
	
	public static class DefaultFunctionKernelBuilderOptionMapper extends FunctionKernelBuilderOptionMapper
	{
		public DefaultFunctionKernelBuilderOptionMapper()
		{
			super(new StringKeyCaseInsenstiveComparer(""));
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			
			register(DefaultFunctionsNames.MAP, new MapOptionsBuilder());

//			register(DefaultFunctionsNames.FILTER, );
//
//			register(DefaultFunctionsNames.REDUCE, );
		}
	}
	
	public static class DefaultOclContextMappings implements IOclContextMappings
	{
		private FunctionKernelBuilderMapper mFunctionKernelBuilderMapping;
		private FunctionKernelBuilderOptionMapper mFunctionKernelBuilderOptionMapper;
		private StreamWriter mStreamWriter;
		private StreamReader mStreamReader;
		
		
		public DefaultOclContextMappings()
		{
			mFunctionKernelBuilderMapping = getDefaultFunctionKernelBuilderMapping();
			
			mFunctionKernelBuilderOptionMapper =
				new DefaultFunctionKernelBuilderOptionMapper();
			
			mStreamWriter = StreamWriter.getStreamWriter();
			mStreamReader = StreamReader.getStreamReader();
		}
		
		@Override
		public FunctionKernelBuilderMapping getFunctionKernelBuilderMapping()
		{
			return null;
		}
		
		@Override
		public FunctionKernelBuilderMapper getFunctionKernelBuilderMapper()
		{
			return mFunctionKernelBuilderMapping;
		}
		
		@Override
		public FunctionKernelBuilderOptionMapper getFunctionKernelBuilderOptionMapper()
		{
			return mFunctionKernelBuilderOptionMapper;
		}
		
		@Override
		public StreamWriter getStreamWriter()
		{
			return mStreamWriter;
		}
		
		@Override
		public StreamReader getStreamReader()
		{
			return mStreamReader;
		}
	}
	
	public static class DefaultUtilityVariableGetter implements KernelBuilder.IUtilityVariablesGetter
	{
		@Override
		public String getUtilityVariables()
		{
			return "unsigned char _arity = _data[0];\n" +
				   "int _i = _dataIndexes[_gId];\n" +
				   "int _userIndex = _i;\n" +
				   "long _l = 0;" +
				   "\n";
		}
	}
	
	private static void enrichTupleKindVarTypeToKernelTypeMapperWithInput(
		TupleKindVarTypeToKernelTypeMapper pMapper,
		KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
	{
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.INPUT_TUPLE, DefaultVarTypes.INT),
			"int");
		
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.INPUT_TUPLE, DefaultVarTypes.DOUBLE),
			"double");
		
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.INPUT_TUPLE, DefaultVarTypes.STRING),
			"__global unsigned char*");
	}
	
	private static void enrichTupleKindVarTypeToKernelTypeMapperWithOutput(
		TupleKindVarTypeToKernelTypeMapper pMapper,
		KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
	{
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultVarTypes.INT),
			"int");
		
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultVarTypes.DOUBLE),
			"double");
		
		pMapper.register(
			pKeyCalculator.getKey(DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultVarTypes.STRING),
			"unsigned char");
	}
	
	private static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper()
	{
		return new TupleKindVarTypeToKernelTypeMapper();
	}
}
