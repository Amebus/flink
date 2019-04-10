package org.apache.flink.streaming.api.ocl.engine.builder.options;

import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TupleKindVarTypeToKernelTypeMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TupleKindsToVariablesGeneratorMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TupleKindsVarTypesToVariableSerializationMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TypeToKernelVariablesLineMapper;
import org.apache.flink.streaming.api.ocl.tuple.Tuple2Ocl;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;
import org.apache.flink.streaming.configuration.ITupleVarDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getStringIterableFromArgs;

public class ReduceOptionsBuilder extends DefaultKernelBuilderOptionsBuilder<KernelBuilderOptions>
{
	@Override
	protected void setKernelParametersList()
	{
		setKernelParametersList(
			getStringIterableFromArgs("__global unsigned char *_data",
									  "__global int *_dataIndexes",
									  "__global unsigned char *_finalResult",
									  "__global unsigned char *_identity",
									  "__global unsigned char *_midResults",
									  "__local unsigned char *_localCache"));
	}
	
	@Override
	protected void setTupleKinds()
	{
		setTupleKinds(DefaultsValues.Reduce.getDefaultTuplesEngineKinds());
	}
	
	@Override
	protected void setUtilityVariablesGetter()
	{
		setUtilityVariablesGetter(new UtilityVariablesGetter());
	}
	
	@Override
	protected void setTypeToKernelVariablesLineMapper()
	{
		TypeToKernelVariablesLineMapper vMapper = new TypeToKernelVariablesLineMapper();
		
		getIterableFromArgs(
			getATupleIntGenerator(),
			getATupleDoubleGenerator(),
			getATupleStringGenerator(),
			getBTupleIntGenerator(),
			getBTupleDoubleGenerator(),
			getBTupleStringGenerator(),
			getIdentityTupleStringGenerator())
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setVarTypeToKernelVariablesLineMapping(vMapper);
	}
	
	@Override
	protected void setTupleKindsVarTypesToVariableSerializationMapping()
	{
		TupleKindsVarTypesToVariableSerializationMapper vMapper = new TupleKindsVarTypesToVariableSerializationMapper();
		
		getIterableFromArgs(
			getAIntSerialization(),
			getADoubleSerialization(),
			getAStringSerialization())
//			getBIntSerialization(),
//			getBDoubleSerialization(),
//			getBStringSerialization())
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setTupleKindsVarTypesToVariableSerializationMapping(vMapper);
	}
	
	@Override
	protected void setDeserializationMacroList()
	{
		setDeserializationMacroList(DefaultsValues.Reduce.getDefaultDeserializationMacrosList());
	}
	
	@Override
	protected void setTupleKindVarTypeToKernelTypeMapping()
	{
		TupleKindVarTypeToKernelTypeMapper vMapping =
			DefaultsValues.Reduce.getTupleKindVarTypeToKernelTypeMapper(getTupleKindVariableTypeKeyCalculator());
		setTupleKindVarTypeToKernelTypeMapping(vMapping);
	}
	
	@Override
	public KernelBuilderOptions build()
	{
		return new KernelBuilderOptions(
			getUtilityFunctionList(),
			getKernelParametersList(),
			getDeserializationMacroList(),
			getSerializationMacroList(),
			getUtilityVariablesGetter(),
			getTupleKindsToVariablesGeneratorMapping(),
			getTupleKinds(),
			getVarTypeToKernelVariablesLineMapping(),
			getVarTypes(),
			getTupleKindVariableTypeKeyCalculator(),
			getTupleKindsVarTypesToVariableDeserializationMapping(),
			getTupleKindsVarTypesToVariableSerializationMapping(),
			getUserFunction(),
			getTupleDefinitionRepository(),
			getContextOptions(),
			getKernelOptions(),
			getTupleKindVarTypeToKernelTypeMapping()
		);
	}
	
	protected void setTupleKindsToVariablesGeneratorMapping()
	{
		TupleKindsToVariablesGeneratorMapper vMapper = new TupleKindsToVariablesGeneratorMapper();
		
		vMapper.register(DefaultsValues.Reduce.LOCAL_TUPLE_A, getAVariableGenerator());
		vMapper.register(DefaultsValues.Reduce.LOCAL_TUPLE_B, getBVariableGenerator());
		vMapper.register(DefaultsValues.Reduce.IDENTITY_TUPLE, getIdentityVariableGenerator());
		
		setTupleKindsToVariablesGeneratorMapping(vMapper);
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getAVariableGenerator()
	{
		return getVariableGenerator("_a");
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getBVariableGenerator()
	{
		return getVariableGenerator("_b");
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getIdentityVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			ITupleDefinition vTuple = pTupleDefinitionRepository.getTupleDefinition(pUserFunction.getInputTupleName());
			List<KernelBuilder.KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = "_slMax" + vVar.getIndex();
							 String vType = vVar.getType();
							 if(vType.startsWith("s"))
							 {
								 vType = DefaultsValues.DefaultVarTypes.STRING;
								 vResult.add(
									 new KernelBuilder
										 .KernelLogicalVariable(vType, vName, vVar.getIndex(), vVar.getMaxReservedBytes()));
							 }
						 });
			return vResult;
		};
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getVariableGenerator(String pVarNamePrefix)
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			ITupleDefinition vTuple = pTupleDefinitionRepository.getTupleDefinition(pUserFunction.getInputTupleName());
			List<KernelBuilder.KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = pVarNamePrefix + vVar.getIndex();
							 addLogicalVarToResult(vResult, vVar, vName);
						 });
			
			return vResult;
		};
	}
	
	protected static class UtilityVariablesGetter extends DefaultsValues.DefaultUtilityVariablesGetter
	{
		@Override
		public String getUtilityVariables(IUserFunction pUserFunction, ITupleDefinitionRepository pRepository)
		{
			ITupleDefinition vTupleDefinition = pRepository.getTupleDefinition(pUserFunction.getInputTupleName());
			return super.getUtilityVariables(pUserFunction, pRepository).replace("_dataIndexes[_gId]", "-1") +
				   "\n\n" +
				   getOclIndexes() +
				   getAdditionalOclVariables() +
				   "\n\n" +
				   getOutputUtilityVariables(vTupleDefinition) +
				   "\n\n" +
				   getAdditionalVariables(vTupleDefinition);
		}
		
		protected String getOclIndexes()
		{
			return "uint _lId = get_local_id(0);\n" +
				   "uint _grId = get_group_id(0);\n";
		}
		
		protected String getAdditionalOclVariables()
		{
			return "uint _grSize = get_local_size(0);\n" +
				   "uint _gSize = get_global_size(0);\n" +
				   "uint _outputCount = get_num_groups(0);\n" +
				   "uint _steps = ceil(log2((double)_gSize)/log2((double)_grSize));\n" +
				   "\n" +
				   "if(_gId < _gSize)\n" +
				   "{\n" +
				   "    _i = _dataIndexes[_gId];\n" +
				   "}\n";
		}
		
		protected String getAdditionalVariables(ITupleDefinition pTupleDefinition)
		{
			return "uint _uiTemp = 0;\n" +
				   "int _iTemp = 0;\n" +
				   "unsigned char _tCounter = 0;\n" +
				   "bool _continueCopy = 1;\n" +
				   "uint _copyLength = 0;\n" +
				   "\n";
		}
		
		protected String getOutputUtilityVariables(ITupleDefinition pTupleDefinition)
		{
			byte vOffset = 0x1;
			vOffset += pTupleDefinition.getArity();
			
			return
				"uint _roff = " + vOffset + ";\n" +
				"uint _otd = " + getTupleDim(pTupleDefinition) + ";\n" +
				getGlobalResultIndex() + "\n" +
				"";
				//getTailResultIndexes(pTupleDefinition) + "\n";
		}
		
		protected int getTupleDim(ITupleDefinition pTupleDefinition)
		{
			return DefaultsValues.getDefaultTupleBytesDimensionGetters().getTupleDimension(pTupleDefinition);
		}
		
		protected String getGlobalResultIndex()
		{
			return "uint _ri = _roff + _grId * _otd;";
		}
		
		private String getTailResultIndexes(ITupleDefinition pTupleDefinition)
		{
			StringBuilder vBuilder = new StringBuilder();
			Iterator<ITupleVarDefinition> vCurrentDefinitionIterator = pTupleDefinition.iterator();
			Iterator<ITupleVarDefinition> vPreviousDefinitionIterator = pTupleDefinition.iterator();
			
			vCurrentDefinitionIterator.next();
			vCurrentDefinitionIterator
				.forEachRemaining(vd -> vBuilder.append(getTailResultIndex(vd, vPreviousDefinitionIterator.next())));
			return vBuilder.toString();
		}
		
		protected String getTailResultIndex(
			ITupleVarDefinition pCurrentDefinition,
			ITupleVarDefinition pPreviousDefinition)
		{
			
			int vPreviousTypeDim = pPreviousDefinition.getMaxReservedBytes();
			if (pPreviousDefinition.getType().startsWith("s"))
			{
				vPreviousTypeDim += 4;
			}
			return "uint " +
				   "_ri" + pCurrentDefinition.getIndex() +
				   " = " +
				   "_ri" + (pPreviousDefinition.getIndex()) +
				   " + " +
				   vPreviousTypeDim +
				   ";\n";
		}
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getATupleIntGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A,  DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				KernelBuilder.KernelVariablesLine vBooleanLine =
					new KernelBuilder.KernelVariablesLine( DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar -> vBooleanLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vBooleanLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getATupleDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A,  DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				KernelBuilder.KernelVariablesLine vBooleanLine =
					new KernelBuilder.KernelVariablesLine( DefaultsValues.DefaultVarTypes.DOUBLE);
				
				pKernelLogicalVariables
					.forEach(pVar -> vBooleanLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vBooleanLine);
				return vResult;
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getATupleStringGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>(2);
				KernelBuilder.KernelVariablesLine vStringLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.STRING);
				
				KernelBuilder.KernelVariablesLine vIntLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar ->
							 {
								 vIntLine.addVarDef("_slA" + pVar.getIndex());
								 vStringLine.addVarDef(pVar.getVarName());
							 });
				
				vResult.add(vIntLine);
				vResult.add(vStringLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getBTupleIntGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B,  DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				KernelBuilder.KernelVariablesLine vBooleanLine =
					new KernelBuilder.KernelVariablesLine( DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar -> vBooleanLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vBooleanLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getBTupleDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B,  DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				KernelBuilder.KernelVariablesLine vBooleanLine =
					new KernelBuilder.KernelVariablesLine( DefaultsValues.DefaultVarTypes.DOUBLE);
				
				pKernelLogicalVariables
					.forEach(pVar -> vBooleanLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vBooleanLine);
				return vResult;
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getBTupleStringGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>(2);
				KernelBuilder.KernelVariablesLine vStringLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.STRING);
				
				KernelBuilder.KernelVariablesLine vIntLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar ->
							 {
								 vIntLine.addVarDef("_slB" + pVar.getIndex());
								 vStringLine.addVarDef(pVar.getVarName());
							 });
				
				vResult.add(vIntLine);
				vResult.add(vStringLine);
				return vResult;
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getIdentityTupleStringGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.IDENTITY_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				
				KernelBuilder.KernelVariablesLine vIntLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar ->
								 vIntLine.addVarDef("_slB" + pVar.getIndex() + " = " + pVar.getBytesDim()));
				
				vResult.add(vIntLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getAIntSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable ->
			{
				String vLine = "SER_INT( #, _iTemp, _localCache, _serializationTemp );"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getADoubleSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable ->
			{
				String vLine = "SER_DOUBLE( #, _iTemp, _localCache, _serializationTemp)"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelSerializationLine(null, pKernelLogicalVariable.getIndex());
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getAStringSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable ->
			{
				//SER_STRING( _r0, _ri0, 12, _result );
				String vLine = "SER_STRING( #, _iTemp, -, _localCache, _serializationTemp );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("-", "" + pKernelLogicalVariable.getBytesDim());
				
				return new KernelBuilder
					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
//	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getBIntSerialization()
//	{
//		return new Tuple2Ocl<>(
//			getTupleKindVariableTypeKeyCalculator()
//				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.INT),
//			pKernelLogicalVariable ->
//			{
//				String vLine = "SER_INT( #, _iTemp, _localCache );"
//					.replace("#", pKernelLogicalVariable.getVarName());
//				return new KernelBuilder
//					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
//			});
//	}
	
//	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getBDoubleSerialization()
//	{
//		return new Tuple2Ocl<>(
//			getTupleKindVariableTypeKeyCalculator()
//				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.DOUBLE),
//			pKernelLogicalVariable ->
//			{
//				String vLine = "SER_DOUBLE( #, _iTemp, _localCache, _l)"
//					.replace("#", pKernelLogicalVariable.getVarName());
//				return new KernelBuilder
//					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
//			});
//	}
	
//	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getBStringSerialization()
//	{
//		return new Tuple2Ocl<>(
//			getTupleKindVariableTypeKeyCalculator()
//				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.STRING),
//			pKernelLogicalVariable ->
//			{
//				//SER_STRING( _r0, _ri0, 12, _result );
//				String vLine = "SER_STRING( #, @, _iTemp, _localCache );"
//					.replace("#", pKernelLogicalVariable.getVarName())
//					.replace("-", "" + pKernelLogicalVariable.getBytesDim());
//
//				return new KernelBuilder
//					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
//			});
//	}
	
	@Override
	protected Iterable<Tuple2Ocl<String, KernelBuilder.IVariableDeserialization>> getKernelVariableDeserialization()
	{
		return getIterableFromArgs(
			getAIntDeserialization(),
			getADoubleDeserialization(),
			getAStringDeserialization(),
			getBIntDeserialization(),
			getBDoubleDeserialization(),
			getBStringDeserialization());
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getAIntDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_INT( _localCache, _iTemp, # );"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getADoubleDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_DOUBLE( _localCache, _iTemp, # , _l);"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getAStringDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_A, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable -> {
				String vLine = "DESER_STRING( _localCache, _iTemp, #, @ );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@", "_slMax" + pKernelLogicalVariable.getIndex());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getBIntDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_INT( _localCache, _iTemp, # );"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getBDoubleDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_DOUBLE( _localCache, _iTemp, # , _l);"
					.replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getBStringDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.Reduce.LOCAL_TUPLE_B, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable -> {
				String vLine = "DESER_STRING( _localCache, _iTemp, #, @ );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@", "_slMax" + pKernelLogicalVariable.getIndex());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
}
