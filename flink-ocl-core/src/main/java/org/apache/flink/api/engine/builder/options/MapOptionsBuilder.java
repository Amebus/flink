package org.apache.flink.api.engine.builder.options;

import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.builder.mappers.TupleKindVarTypeToKernelTypeMapper;
import org.apache.flink.api.engine.builder.mappers.TupleKindsVarTypesToVariableSerializationMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.KernelBuilderOptions;
import org.apache.flink.api.engine.builder.mappers.TypeToKernelVariablesLineMapper;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.ITupleDefinitionRepository;
import org.apache.flink.configuration.ITupleVarDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.api.common.utility.IterableHelper.getIterableFromArgs;

public class MapOptionsBuilder extends DefaultKernelBuilderOptionsBuilder<KernelBuilderOptions>
{
	@Override
	protected void setUtilityVariablesGetter()
	{
		setUtilityVariablesGetter(new UtilityVariablesGetter());
	}
	
	@Override
	protected void setTypeToKernelVariablesLineMapper()
	{
		TypeToKernelVariablesLineMapper vMapper = new TypeToKernelVariablesLineMapper();
		
		getKernelVariableGenerators()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setVarTypeToKernelVariablesLineMapping(vMapper);
	}
	
	@Override
	protected void setTupleKindsVarTypesToVariableSerializationMapping()
	{
		TupleKindsVarTypesToVariableSerializationMapper vMapper = new TupleKindsVarTypesToVariableSerializationMapper();
		
		getKernelVariableSerialization()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setTupleKindsVarTypesToVariableSerializationMapping(vMapper);
	}
	
	@Override
	protected void setTupleKindVarTypeToKernelTypeMapping()
	{
		TupleKindVarTypeToKernelTypeMapper vMapping =
			DefaultsValues.Map.getTupleKindVarTypeToKernelTypeMapper(getTupleKindVariableTypeKeyCalculator());
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
	
	private Iterable<Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator>> getKernelVariableGenerators()
	{
		return getIterableFromArgs(
			getInputIntGenerator(),
			getInputDoubleGenerator(),
			getInputStringGenerator(),
			getOutputIntGenerator(),
			getOutputDoubleGenerator(),
			getOutputStringGenerator());
	}
	
	private Iterable<Tuple2Ocl<String, KernelBuilder.IVariableSerialization>> getKernelVariableSerialization()
	{
		return getIterableFromArgs(
			getIntSerialization(),
			getDoubleSerialization(),
			getStringSerialization());
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputIntGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>(2);
				KernelBuilder.KernelVariablesLine vIntLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.INT);
				
				pKernelLogicalVariables
					.forEach(pVar -> vIntLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vIntLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>(2);
				KernelBuilder.KernelVariablesLine vDoubleLine =
					new KernelBuilder.KernelVariablesLine(DefaultsValues.DefaultVarTypes.DOUBLE);
				
				pKernelLogicalVariables
					.forEach(pVar -> vDoubleLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vDoubleLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputStringGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
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
								 vIntLine.addVarDef("_rsl" + pVar.getIndex() +
													" = " + pVar.getBytesDim());
								 vStringLine.addVarDef("_r" + pVar.getIndex() +
													   "[" + pVar.getBytesDim() + "]");
							 });
				
				vResult.add(vIntLine);
				vResult.add(vStringLine);
				return vResult;
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getIntSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable ->
			{
				String vLine = "SER_INT( #, @, _result );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@","_ri" + pKernelLogicalVariable.getIndex());
				return new KernelBuilder
					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getDoubleSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable ->
			{
				String vLine = "SER_DOUBLE( #, @, _result, _l)"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@", "_ri" + pKernelLogicalVariable.getIndex());
				return new KernelBuilder
					.KernelSerializationLine(null, pKernelLogicalVariable.getIndex());
			});
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getStringSerialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable ->
			{
				//SER_STRING( _r0, _ri0, 12, _result );
				String vLine = "SER_STRING( #, @, -, _result );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@", "_ri" + pKernelLogicalVariable.getIndex())
					.replace("-", "" + pKernelLogicalVariable.getBytesDim());
				
				return new KernelBuilder
					.KernelSerializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	private static class UtilityVariablesGetter extends DefaultsValues.DefaultUtilityVariablesGetter
	{
		
		@Override
		public String getUtilityVariables(IUserFunction pUserFunction, ITupleDefinitionRepository pRepository)
		{
			return super.getUtilityVariables(pUserFunction, pRepository) +
				   "\n\n" +
				   getOutputUtilityVariables(pRepository.getTupleDefinition(pUserFunction.getOutputTupleName()));
		}
		
		protected String getOutputUtilityVariables(ITupleDefinition pTupleDefinition)
		{
			byte vOffset = 0x1;
			vOffset += pTupleDefinition.getArity();
			
			return
				"uint _roff = " + vOffset + ";\n" +
				"uint _otd = " + getTupleDim(pTupleDefinition) + ";\n" +
				getFirstResultIndex() + "\n" +
				getTailResultIndexes(pTupleDefinition) + "\n";
		}
		
		protected int getTupleDim(ITupleDefinition pTupleDefinition)
		{
			return DefaultsValues.getDefaultTupleBytesDimensionGetters().getTupleDimension(pTupleDefinition);
		}
		
		protected String getFirstResultIndex()
		{
			return "uint _ri0 = _roff + _gId * _otd;";
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
}
