package org.apache.flink.api.newEngine.kernel.builder.options;

import org.apache.flink.api.newEngine.kernel.builder.KernelBuilder;
import org.apache.flink.api.newEngine.kernel.builder.KernelBuilderOptions;
import org.apache.flink.api.newEngine.kernel.builder.mappers.*;
import org.apache.flink.api.tuple.Tuple2Ocl;

import java.util.ArrayList;

import static org.apache.flink.api.common.utility.IterableHelper.getIterableFromArgs;

public class MapOptionsBuilder extends KernelBuilderOptionsBuilder<KernelBuilderOptions>
{
	
	public MapOptionsBuilder()
	{
		this(true);
	}
	
	protected MapOptionsBuilder(boolean pSetUpDefaultState)
	{
		if(pSetUpDefaultState)
			setDefaultState();
	}
	
	protected void setDefaultState()
	{
		setKeyCalculator();
		setUtilityFunctions();
		setKernelParametersList();
		setUtilityVariablesGetter();
		setTupleKindsToVariablesGeneratorMapping();
		setTupleKinds();
		setTypeToKernelVariablesLineMapper();
		setVarTypes();
		setTupleKindsVarTypesToVariableDeserializationMapping();
		setTupleKindsVarTypesToVariableSerializationMapping();
		setTupleKindVarTypeToKernelTypeMapping();
	}
	
	protected void setUtilityFunctions()
	{
		setUtilityFunctionList(DefaultsValues.getDefaultUtilityFunctions());
	}
	
	protected void setKernelParametersList()
	{
		setKernelParametersList(DefaultsValues.getDefaultKernelParameterList());
	}
	
	protected void setKeyCalculator()
	{
		setTupleKindVariableTypeKeyCalculator(DefaultsValues.getTupleKindVariableTypeKeyCalculator());
	}
	
	protected void setUtilityVariablesGetter()
	{
		setUtilityVariablesGetter(DefaultsValues.getDefaultUtilityVariableGetter());
	}
	
	protected void setTupleKindsToVariablesGeneratorMapping()
	{
		TupleKindsToVariablesGeneratorMapper vMapper = new TupleKindsToVariablesGeneratorMapper();
		
		vMapper.register(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, getDefaultInputVariableGenerator());
		vMapper.register(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, getDefaultOutputVariableGenerator());
		
		setTupleKindsToVariablesGeneratorMapping(vMapper);
	}
	
	protected void setTupleKinds()
	{
		setTupleKinds(DefaultsValues.getDefaultTuplesEngineKinds());
	}
	
	protected void setTypeToKernelVariablesLineMapper()
	{
		TypeToKernelVariablesLineMapper vMapper = new TypeToKernelVariablesLineMapper();
		
		getKernelVariableGenerators()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setVarTypeToKernelVariablesLineMapping(vMapper);
	}
	
	protected void setVarTypes()
	{
		setVarTypes(DefaultsValues.getDefaultVarTypes());
	}
	
	protected void setTupleKindsVarTypesToVariableDeserializationMapping()
	{
		TupleKindsVarTypesToVariableDeserializationMapper vMapper = new TupleKindsVarTypesToVariableDeserializationMapper();
		
		getKernelVariableDeserialization()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setTupleKindsVarTypesToVariableDeserializationMapping(vMapper);
	}
	
	protected void setTupleKindsVarTypesToVariableSerializationMapping()
	{
		TupleKindsVarTypesToVariableSerializationMapper vMapper = new TupleKindsVarTypesToVariableSerializationMapper();
		
		getKernelVariableSerialization()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setTupleKindsVarTypesToVariableSerializationMapping(vMapper);
	}
	
	private void setTupleKindVarTypeToKernelTypeMapping()
	{
		TupleKindVarTypeToKernelTypeMapper vMapping = DefaultsValues.Map.getTupleKindVarTypeToKernelTypeMapper(getTupleKindVariableTypeKeyCalculator());
		setTupleKindVarTypeToKernelTypeMapping(vMapping);
	}
	
	@Override
	public KernelBuilderOptions build()
	{
		 return new KernelBuilderOptions(
			 getUtilityFunctionList(),
			 getKernelParametersList(),
			 getUtilityVariablesGetter(),
			 getTupleKindsToVariablesGeneratorMapping(),
			 getTupleKinds(),
			 getVarTypeToKernelVariablesLineMapping(),
			 getVarTypes(),
			 getTupleKindVariableTypeKeyCalculator(),
			 getTupleKindsVarTypesToVariableDeserializationMapping(),
			 getTupleKindsVarTypesToVariableSerializationMapping(),
			 getUserFunction(),
			 getTupleDefinitionsRepository(),
			 getContextOptions(),
			 getKernelOptions(),
			 getTupleKindVarTypeToKernelTypeMapping()
		);
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getDefaultInputVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			return new ArrayList<>();
		};
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getDefaultOutputVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			return new ArrayList<>();
		};
	}
	
	protected KernelBuilder.ITupleKindVariableTypeKeyCalculator getKeyCalculator()
	{
		return getTupleKindVariableTypeKeyCalculator();
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
	
	private Iterable<Tuple2Ocl<String, KernelBuilder.IVariableDeserialization>> getKernelVariableDeserialization()
	{
		return getIterableFromArgs(
			getIntDeserialization(),
			getDoubleDeserialization(),
			getStringDeserialization());
	}
	
	private Iterable<Tuple2Ocl<String, KernelBuilder.IVariableSerialization>> getKernelVariableSerialization()
	{
		return getIterableFromArgs(
			getIntSerialization(),
			getDoubleSerialization(),
			getStringSerialization());
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputIntGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputStringGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputIntGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getOutputStringGenerator()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariables -> new KernelBuilder.KernelVariablesLine(null));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getIntDeserialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelDeserializationLine(null, pKernelLogicalVariable.getIndex()));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getDoubleDeserialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelDeserializationLine(null, pKernelLogicalVariable.getIndex()));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getStringDeserialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelDeserializationLine(null, pKernelLogicalVariable.getIndex()));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getIntSerialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelSerializationLine(null, pKernelLogicalVariable.getIndex()));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getDoubleSerialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelSerializationLine(null, pKernelLogicalVariable.getIndex()));
	}
	
	private Tuple2Ocl<String, KernelBuilder.IVariableSerialization> getStringSerialization()
	{
		return new Tuple2Ocl<>(
			getKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable -> new KernelBuilder
				.KernelSerializationLine(null, pKernelLogicalVariable.getIndex()));
	}
}
