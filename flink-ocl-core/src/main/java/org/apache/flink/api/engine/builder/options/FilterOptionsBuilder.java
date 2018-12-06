package org.apache.flink.api.engine.builder.options;

import org.apache.flink.api.engine.builder.mappers.TupleKindsVarTypesToVariableSerializationMapper;
import org.apache.flink.api.engine.builder.mappers.TypeToKernelVariablesLineMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.KernelBuilderOptions;
import org.apache.flink.api.engine.builder.mappers.TupleKindVarTypeToKernelTypeMapper;
import org.apache.flink.api.tuple.Tuple2Ocl;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.common.utility.IterableHelper.getIterableFromArgs;

public class FilterOptionsBuilder extends DefaultKernelBuilderOptionsBuilder<KernelBuilderOptions>
{
	
	protected String getBooleanType()
	{
		return DefaultsValues.Filter.BOOLEAN;
	}
	
	@Override
	protected void setVarTypes()
	{
		setVarTypes(DefaultsValues.Filter.getDefaultVarTypes());
	}
	
	@Override
	protected void setUtilityVariablesGetter()
	{
		setUtilityVariablesGetter(DefaultsValues.getDefaultUtilityVariableGetter());
	}
	
	@Override
	protected void setTypeToKernelVariablesLineMapper()
	{
		TypeToKernelVariablesLineMapper vMapper = new TypeToKernelVariablesLineMapper();
		
		getIterableFromArgs(
			getInputIntGenerator(),
			getInputDoubleGenerator(),
			getInputStringGenerator(),
			getOutputGenerator())
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setVarTypeToKernelVariablesLineMapping(vMapper);
	}
	
	@Override
	protected void setTupleKindsVarTypesToVariableSerializationMapping()
	{
		TupleKindsVarTypesToVariableSerializationMapper vMapper = new TupleKindsVarTypesToVariableSerializationMapper();
		
		String vKey = getTupleKindVariableTypeKeyCalculator()
			.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, getBooleanType());
		
		vMapper.register(vKey, pKernelLogicalVariable ->
			new KernelBuilder.KernelSerializationLine("_result[_gId] = _r0;", 0));
		
		setTupleKindsVarTypesToVariableSerializationMapping(vMapper);
	}
	
	@Override
	protected void setTupleKindVarTypeToKernelTypeMapping()
	{
		TupleKindVarTypeToKernelTypeMapper vMapping =
			DefaultsValues.Filter.getTupleKindVarTypeToKernelTypeMapper(getTupleKindVariableTypeKeyCalculator());
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
	
	@Override
	protected KernelBuilder.IKernelVariablesGenerator getOutputVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			List<KernelBuilder.KernelLogicalVariable> vResult = new ArrayList<>();
			
			vResult
				.add(new KernelBuilder.KernelLogicalVariable("bool", "_r0", 0, 1));
			
			return vResult;
		};
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, getBooleanType()),
			pKernelLogicalVariables ->
			{
				List<KernelBuilder.KernelVariablesLine> vResult = new ArrayList<>();
				KernelBuilder.KernelVariablesLine vBooleanLine =
					new KernelBuilder.KernelVariablesLine(getBooleanType());
				
				pKernelLogicalVariables
					.forEach(pVar -> vBooleanLine.addVarDef(pVar.getVarName()));
				
				vResult.add(vBooleanLine);
				return vResult;
			});
	}
}
