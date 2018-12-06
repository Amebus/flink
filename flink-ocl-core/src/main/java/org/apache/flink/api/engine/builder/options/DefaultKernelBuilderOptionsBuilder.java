package org.apache.flink.api.engine.builder.options;

import org.apache.flink.api.engine.builder.mappers.TupleKindsToVariablesGeneratorMapper;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.KernelBuilderOptions;
import org.apache.flink.api.engine.builder.mappers.TupleKindsVarTypesToVariableDeserializationMapper;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.apache.flink.newConfiguration.ITupleDefinition;
import org.apache.flink.newConfiguration.ITupleVarDefinition;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.api.common.utility.IterableHelper.getIterableFromArgs;

public abstract class DefaultKernelBuilderOptionsBuilder<T extends KernelBuilderOptions> extends KernelBuilderOptionsBuilder<T>
{
	
	public DefaultKernelBuilderOptionsBuilder()
	{
		setInternalState();
	}
	
	protected void setInternalState()
	{
		setKeyCalculator();
		setKernelParametersList();
		setUtilityFunctions();
		setDeserializationMacroList();
		setSerializationMacroList();
		setUtilityVariablesGetter();
		setTupleKindsToVariablesGeneratorMapping();
		setTupleKinds();
		setVarTypes();
		setTypeToKernelVariablesLineMapper();
		setTupleKindsVarTypesToVariableDeserializationMapping();
		setTupleKindsVarTypesToVariableSerializationMapping();
		setTupleKindVarTypeToKernelTypeMapping();
	}
	
	protected void setKeyCalculator()
	{
		setTupleKindVariableTypeKeyCalculator(DefaultsValues.getTupleKindVariableTypeKeyCalculator());
	}
	
	protected void setKernelParametersList()
	{
		setKernelParametersList(DefaultsValues.getDefaultKernelParameterList());
	}
	
	protected void setUtilityFunctions()
	{
		setUtilityFunctionList(DefaultsValues.getDefaultUtilityFunctions());
	}
	
	protected void setDeserializationMacroList()
	{
		setDeserializationMacroList(DefaultsValues.getDefaultDeserializationMacrosList());
	}
	
	protected void setSerializationMacroList()
	{
		setSerializationMacroList(DefaultsValues.getDefaultSerializationMacrosList());
	}
	
	protected void setTupleKinds()
	{
		setTupleKinds(DefaultsValues.getDefaultTuplesEngineKinds());
	}
	
	protected void setVarTypes()
	{
		setVarTypes(DefaultsValues.getDefaultVarTypes());
	}
	
	protected abstract void setUtilityVariablesGetter();
	
	protected void setTupleKindsToVariablesGeneratorMapping()
	{
		TupleKindsToVariablesGeneratorMapper vMapper = new TupleKindsToVariablesGeneratorMapper();
		
		vMapper.register(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, getInputVariableGenerator());
		vMapper.register(DefaultsValues.DefaultsTuplesKinds.OUTPUT_TUPLE, getOutputVariableGenerator());
		
		setTupleKindsToVariablesGeneratorMapping(vMapper);
	}
	
	protected abstract void setTypeToKernelVariablesLineMapper();
	
	protected abstract void setTupleKindsVarTypesToVariableSerializationMapping();
	
	protected abstract void setTupleKindVarTypeToKernelTypeMapping();
	
	protected void setTupleKindsVarTypesToVariableDeserializationMapping()
	{
		TupleKindsVarTypesToVariableDeserializationMapper vMapper = new TupleKindsVarTypesToVariableDeserializationMapper();
		
		getKernelVariableDeserialization()
			.forEach(pGenerator -> vMapper.register(pGenerator.f0, pGenerator.f1));
		
		setTupleKindsVarTypesToVariableDeserializationMapping(vMapper);
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getInputVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			ITupleDefinition vTuple = pTupleDefinitionRepository.getTupleDefinition(pUserFunction.getInputTupleName());
			List<KernelBuilder.KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = "_t" + vVar.getIndex();
							 addLogicalVarToResult(vResult, vVar, vName);
						 });
			
			return vResult;
		};
	}
	
	protected KernelBuilder.IKernelVariablesGenerator getOutputVariableGenerator()
	{
		return (pUserFunction, pTupleDefinitionRepository) ->
		{
			ITupleDefinition vTuple = pTupleDefinitionRepository.getTupleDefinition(pUserFunction.getOutputTupleName());
			List<KernelBuilder.KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = "_r" + vVar.getIndex();
							 addLogicalVarToResult(vResult, vVar, vName);
						 });
			
			return vResult;
		};
	}
	
	protected void addLogicalVarToResult(
		List<KernelBuilder.KernelLogicalVariable> pResult,
		ITupleVarDefinition vVar,
		String pName)
	{
		String vType = vVar.getType().toLowerCase();
		
		if(vType.startsWith("i"))
		{
			vType = DefaultsValues.DefaultVarTypes.INT;
		}
		else if(vType.startsWith("d"))
		{
			vType = DefaultsValues.DefaultVarTypes.DOUBLE;
		}
		else if(vType.startsWith("s"))
		{
			vType = DefaultsValues.DefaultVarTypes.STRING;
			
		}
		
		pResult.add(
			new KernelBuilder.KernelLogicalVariable(vType, pName, vVar.getIndex(), vVar.getMaxReservedBytes()));
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputIntGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
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
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputDoubleGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
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
	
	protected Tuple2Ocl<String, KernelBuilder.IKernelVariablesLineGenerator> getInputStringGenerator()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
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
								 vIntLine.addVarDef("_sl" + pVar.getIndex());
								 vStringLine.addVarDef(pVar.getVarName());
							 });
				
				vResult.add(vIntLine);
				vResult.add(vStringLine);
				return vResult;
			});
	}
	
	protected Iterable<Tuple2Ocl<String, KernelBuilder.IVariableDeserialization>> getKernelVariableDeserialization()
	{
		return getIterableFromArgs(
			getIntDeserialization(),
			getDoubleDeserialization(),
			getStringDeserialization());
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getIntDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.INT),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_INT( _data, _i, # );".replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getDoubleDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.DOUBLE),
			pKernelLogicalVariable ->
			{
				String vLine = "DESER_DOUBLE( _data, _i, # , _l);".replace("#", pKernelLogicalVariable.getVarName());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
	
	protected Tuple2Ocl<String, KernelBuilder.IVariableDeserialization> getStringDeserialization()
	{
		return new Tuple2Ocl<>(
			getTupleKindVariableTypeKeyCalculator()
				.getKey(DefaultsValues.DefaultsTuplesKinds.INPUT_TUPLE, DefaultsValues.DefaultVarTypes.STRING),
			pKernelLogicalVariable -> {
				String vLine = "DESER_STRING( _data, _i, #, @ );"
					.replace("#", pKernelLogicalVariable.getVarName())
					.replace("@", "_sl" + pKernelLogicalVariable.getIndex());
				return new KernelBuilder
					.KernelDeserializationLine(vLine, pKernelLogicalVariable.getIndex());
			});
	}
}
