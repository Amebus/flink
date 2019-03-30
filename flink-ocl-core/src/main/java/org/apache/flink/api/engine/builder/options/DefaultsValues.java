package org.apache.flink.api.engine.builder.options;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.bridge.identity.BigEndianIdentityValuesConverter;
import org.apache.flink.api.bridge.identity.LittleEndianIdentityValuesConverter;
import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.IOclContextMappings;
import org.apache.flink.api.engine.ITupleBytesDimensionGetters;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.ReduceKernelBuilder;
import org.apache.flink.api.engine.builder.mappers.*;
import org.apache.flink.api.serialization.bigendian.BigEndianStreamReader;
import org.apache.flink.api.serialization.bigendian.BigEndianStreamWriter;
import org.apache.flink.api.serialization.littleendian.LittleEndianStreamReader;
import org.apache.flink.api.serialization.littleendian.LittleEndianStreamWriter;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.StreamWriter;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionRepository;

import java.nio.ByteOrder;

import static org.apache.flink.api.common.utility.IterableHelper.getIterableFromArgs;
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
	
	public static Iterable<String> getDefaultDeserializationMacrosList()
	{
		return getIterableFromArgs(
			DefaultDeserializationMacros.DESER_INT,
			DefaultDeserializationMacros.DESER_DOUBLE,
			DefaultDeserializationMacros.DESER_STRING);
	}
	
	public static Iterable<String> getDefaultSerializationMacrosList()
	{
		return getIterableFromArgs(
			DefaultSerializationMacros.SER_INT,
			DefaultSerializationMacros.SER_DOUBLE,
			DefaultSerializationMacros.SER_STRING);
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
		return new DefaultUtilityVariablesGetter();
	}
	
	public static KernelBuilder.ITupleKindVariableTypeKeyCalculator getTupleKindVariableTypeKeyCalculator()
	{
		return (pTupleKinds, pVarType) -> pTupleKinds + "-" + pVarType;
	}
	
	public static FunctionKernelBuilderMapper getDefaultFunctionKernelBuilderMapping()
	{
		return new DefaultFunctionKernelBuilderMapping();
	}
	
	public static ITupleBytesDimensionGetters getDefaultTupleBytesDimensionGetters()
	{
		return pTupleDefinition ->
		{
			final int[] vResult = {0};
			pTupleDefinition
				.forEach( pVar ->
						  {
							  vResult[0] +=pVar.getMaxReservedBytes();
							  if(pVar.getType().startsWith("s"))
							  {
								  vResult[0]+=4;
							  }
						  });
			return vResult[0];
		};
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
		public static final String BOOLEAN = "bool";
		
		public static Iterable<String> getDefaultVarTypes()
		{
			return getStringIterableFromArgs(DefaultVarTypes.INT,
											 DefaultVarTypes.DOUBLE,
											 DefaultVarTypes.STRING,
											 BOOLEAN);
		}
		
		public static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper(
			KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
		{
			TupleKindVarTypeToKernelTypeMapper vMapper = DefaultsValues.getTupleKindVarTypeToKernelTypeMapper();
			
			DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithInput(vMapper, pKeyCalculator);
			//DefaultsValues.enrichTupleKindVarTypeToKernelTypeMapperWithOutput(vMapper, pKeyCalculator);
			vMapper.register(
				pKeyCalculator.getKey(DefaultsTuplesKinds.OUTPUT_TUPLE, BOOLEAN),
				"unsigned char");
			
			return vMapper;
		}
	}
	
	public static class Reduce
	{
		public static final String LOCAL_TUPLE_A = "local-a";
		public static final String LOCAL_TUPLE_B = "local-b";
		public static final String IDENTITY_TUPLE = "identity";
		
		public static Iterable<String> getDefaultTuplesEngineKinds()
		{
			return getStringIterableFromArgs(LOCAL_TUPLE_A,
											 LOCAL_TUPLE_B,
											 IDENTITY_TUPLE);
		}
		
		public static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper(
			KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator)
		{
			TupleKindVarTypeToKernelTypeMapper vMapper = DefaultsValues.getTupleKindVarTypeToKernelTypeMapper();
			
			enrichMapper(pKeyCalculator, vMapper, LOCAL_TUPLE_A);
			
			enrichMapper(pKeyCalculator, vMapper, LOCAL_TUPLE_B);
			
			vMapper.register(
				pKeyCalculator.getKey(IDENTITY_TUPLE, DefaultVarTypes.INT),
				"int");
			
			return vMapper;
		}
		
		private static void enrichMapper(KernelBuilder.ITupleKindVariableTypeKeyCalculator pKeyCalculator,
										 TupleKindVarTypeToKernelTypeMapper pMapper,
										 String pTupleKind)
		{
			pMapper.register(
				pKeyCalculator.getKey(pTupleKind, DefaultVarTypes.INT),
				"int");
			
			pMapper.register(
				pKeyCalculator.getKey(pTupleKind, DefaultVarTypes.DOUBLE),
				"double");
			
			pMapper.register(
				pKeyCalculator.getKey(pTupleKind, DefaultVarTypes.STRING),
				"__local unsigned char*");
		}
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
	
	public static class DefaultDeserializationMacros
	{
		public static final int DESERIALIZATION_MACROS_COUNT = 3;
		
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
	
	public static class DefaultSerializationMacros
	{
		public static final int SERIALIZATION_MACROS_COUNT = 3;
		
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
			register(DefaultFunctionsNames.REDUCE, ReduceKernelBuilder::new);
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

			register(DefaultFunctionsNames.FILTER, new FilterOptionsBuilder());

			register(DefaultFunctionsNames.REDUCE, new ReduceOptionsBuilder());
		}
	}
	
	public static class DefaultNumbersByteOrderingStreamWriterMapper extends NumbersByteOrderingStreamWriterMapper
	{
		public DefaultNumbersByteOrderingStreamWriterMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, LittleEndianStreamWriter::new);
			register(ByteOrder.BIG_ENDIAN, BigEndianStreamWriter::new);
		}
	}
	
	public static class DefaultNumbersByteOrderingStreamReaderMapper extends NumbersByteOrderingStreamReaderMapper
	{
		public DefaultNumbersByteOrderingStreamReaderMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, LittleEndianStreamReader::new);
			register(ByteOrder.BIG_ENDIAN, BigEndianStreamReader::new);
		}
	}
	
	public static class DefaultNumbersByteOrderingToIdentityValuesConverterMapper
		extends NumbersByteOrderingToIdentityValuesConverterMapper
	{
		public DefaultNumbersByteOrderingToIdentityValuesConverterMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, new LittleEndianIdentityValuesConverter());
			register(ByteOrder.BIG_ENDIAN, new BigEndianIdentityValuesConverter());
		}
	}
	
	public static class DefaultOclContextMappings implements IOclContextMappings
	{
		protected FunctionKernelBuilderMapper mFunctionKernelBuilderMapping;
		protected FunctionKernelBuilderOptionMapper mFunctionKernelBuilderOptionMapper;
		protected ITupleBytesDimensionGetters mTupleBytesDimensionGetters;
		
		protected NumbersByteOrderingStreamWriterMapper mNumbersByteOrderingStreamWriterMapper;
		protected NumbersByteOrderingStreamReaderMapper mNumbersByteOrderingStreamReaderMapper;
		protected NumbersByteOrderingToIdentityValuesConverterMapper mNumbersByteOrderingToIdentityValuesConverterMapper;
		
		public DefaultOclContextMappings()
		{
			mFunctionKernelBuilderMapping = getDefaultFunctionKernelBuilderMapping();
			
			mFunctionKernelBuilderOptionMapper =
				new DefaultFunctionKernelBuilderOptionMapper();
			
			mTupleBytesDimensionGetters = getDefaultTupleBytesDimensionGetters();
			
			mNumbersByteOrderingStreamWriterMapper = new DefaultNumbersByteOrderingStreamWriterMapper();
			mNumbersByteOrderingStreamReaderMapper = new DefaultNumbersByteOrderingStreamReaderMapper();
			mNumbersByteOrderingToIdentityValuesConverterMapper = new DefaultNumbersByteOrderingToIdentityValuesConverterMapper();
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
		public StringKeyMapper<Byte> getVarTypeToSerializationTypeMapper()
		{
			StringKeyMapper<Byte> vMapper = new StringKeyMapper<>(new StringKeyCaseInsenstiveComparer(""));
			
			vMapper.register(DefaultVarTypes.INT, DefaultsSerializationTypes.INT);
			vMapper.register(DefaultVarTypes.DOUBLE, DefaultsSerializationTypes.DOUBLE);
			vMapper.register(DefaultVarTypes.STRING, DefaultsSerializationTypes.STRING);
			
			return vMapper;
		}
		
		@Override
		public ITupleBytesDimensionGetters getTupleBytesDimensionGetters()
		{
			return mTupleBytesDimensionGetters;
		}
		
		@Override
		public NumbersByteOrderingStreamWriterMapper getNumbersByteOrderingStreamWriterMapper()
		{
			return mNumbersByteOrderingStreamWriterMapper;
		}
		
		@Override
		public NumbersByteOrderingStreamReaderMapper getNumbersByteOrderingStreamReaderMapper()
		{
			return mNumbersByteOrderingStreamReaderMapper;
		}
		
		@Override
		public NumbersByteOrderingToIdentityValuesConverterMapper getByteOrderingToIdentityValuesConverterMapper()
		{
			return mNumbersByteOrderingToIdentityValuesConverterMapper;
		}
	}
	
	public static class DefaultUtilityVariablesGetter implements KernelBuilder.IUtilityVariablesGetter
	{
		
		@Override
		public String getUtilityVariables(IUserFunction pUserFunction, ITupleDefinitionRepository pRepository)
		{
			byte vArity = pRepository.getTupleDefinition(pUserFunction.getInputTupleName()).getArity();
			return "unsigned char _arity = " + vArity + ";\n" +
				   "int _i = _dataIndexes[_gId];\n" +
				   "int _userIndex = _i;\n" +
				   "unsigned char* _serializationTemp;\n" +
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
			"__global char*");
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
			"char");
	}
	
	private static TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapper()
	{
		return new TupleKindVarTypeToKernelTypeMapper();
	}
}
