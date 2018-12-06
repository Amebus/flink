package org.apache.flink.api.engine.builder.options;

import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.builder.mappers.*;
import org.apache.flink.api.engine.builder.IKernelBuilderOptionsBuilder;
import org.apache.flink.api.engine.builder.KernelBuilder;
import org.apache.flink.api.engine.builder.KernelBuilderOptions;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.configuration.ITupleDefinitionRepository;

public abstract class KernelBuilderOptionsBuilder<T extends KernelBuilderOptions> implements IKernelBuilderOptionsBuilder<T>
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	private Iterable<String> mDeserializationMacroList;
	private Iterable<String> mSerializationMacroList;
	
	private Iterable<String> mUtilityFunctionList;
	private Iterable<String> mKernelParametersList;
	private KernelBuilder.IUtilityVariablesGetter mUtilityVariablesGetter;
	
	private TupleKindsToVariablesGeneratorMapper mTupleKindsToVariablesGeneratorMapping;
	private Iterable<String> mTupleKinds;
	
	private TypeToKernelVariablesLineMapper mVarTypeToKernelVariablesLineMapping;
	private Iterable<String> mVarTypes;
	
	private KernelBuilder.ITupleKindVariableTypeKeyCalculator mTupleKindVariableTypeKeyCalculator;
	private TupleKindsVarTypesToVariableDeserializationMapper mTupleKindsVarTypesToVariableDeserializationMapping;
	private TupleKindsVarTypesToVariableSerializationMapper mTupleKindsVarTypesToVariableSerializationMapping;
	
	private TupleKindVarTypeToKernelTypeMapper mTupleKindVarTypeToKernelTypeMapping;
	
	
	@Override
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	@Override
	public ITupleDefinitionRepository getTupleDefinitionRepository()
	{
		return mTupleDefinitionRepository;
	}
	
	@Override
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	
	@Override
	public IOclKernelsOptions getKernelOptions()
	{
		return mKernelOptions;
	}
	
	
	
	@Override
	public IKernelBuilderOptionsBuilder<T> setUserFunction(IUserFunction pUserFunction)
	{
		mUserFunction = pUserFunction;
		return this;
	}
	
	@Override
	public IKernelBuilderOptionsBuilder<T> setTupleDefinitionRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
	{
		mTupleDefinitionRepository = pTupleDefinitionsRepository;
		return this;
	}
	
	@Override
	public IKernelBuilderOptionsBuilder<T> setContextOptions(IOclContextOptions pContextOptions)
	{
		mContextOptions = pContextOptions;
		return this;
	}
	
	@Override
	public IKernelBuilderOptionsBuilder<T> setKernelOptions(IOclKernelsOptions pKernelOptions)
	{
		mKernelOptions = pKernelOptions;
		return this;
	}
	
	
	
	protected Iterable<String> getUtilityFunctionList()
	{
		return mUtilityFunctionList;
	}
	
	protected Iterable<String> getKernelParametersList()
	{
		return mKernelParametersList;
	}
	
	public Iterable<String> getDeserializationMacroList()
	{
		return mDeserializationMacroList;
	}
	
	public Iterable<String> getSerializationMacroList()
	{
		return mSerializationMacroList;
	}
	
	protected KernelBuilder.IUtilityVariablesGetter getUtilityVariablesGetter()
	{
		return mUtilityVariablesGetter;
	}
	
	protected Iterable<String> getTupleKinds()
	{
		return mTupleKinds;
	}
	
	protected TupleKindsToVariablesGeneratorMapper getTupleKindsToVariablesGeneratorMapping()
	{
		return mTupleKindsToVariablesGeneratorMapping;
	}
	
	protected TypeToKernelVariablesLineMapper getVarTypeToKernelVariablesLineMapping()
	{
		return mVarTypeToKernelVariablesLineMapping;
	}
	
	protected Iterable<String> getVarTypes()
	{
		return mVarTypes;
	}
	
	protected KernelBuilder.ITupleKindVariableTypeKeyCalculator getTupleKindVariableTypeKeyCalculator()
	{
		return mTupleKindVariableTypeKeyCalculator;
	}
	
	protected TupleKindsVarTypesToVariableDeserializationMapper getTupleKindsVarTypesToVariableDeserializationMapping()
	{
		return mTupleKindsVarTypesToVariableDeserializationMapping;
	}
	
	protected TupleKindsVarTypesToVariableSerializationMapper getTupleKindsVarTypesToVariableSerializationMapping()
	{
		return mTupleKindsVarTypesToVariableSerializationMapping;
	}
	
	protected TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapping()
	{
		return mTupleKindVarTypeToKernelTypeMapping;
	}
	
	
	
	public KernelBuilderOptionsBuilder<T> setUtilityFunctionList(Iterable<String> pUtilityFunctionList)
	{
		mUtilityFunctionList = pUtilityFunctionList;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setKernelParametersList(Iterable<String> pKernelParametersList)
	{
		mKernelParametersList = pKernelParametersList;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setDeserializationMacroList(Iterable<String> pDeserializationMacroList)
	{
		mDeserializationMacroList = pDeserializationMacroList;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setSerializationMacroList(Iterable<String> pSerializationMacroList)
	{
		mSerializationMacroList = pSerializationMacroList;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setUtilityVariablesGetter(KernelBuilder.IUtilityVariablesGetter pUtilityVariablesGetter)
	{
		mUtilityVariablesGetter = pUtilityVariablesGetter;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKindsToVariablesGeneratorMapping(
		TupleKindsToVariablesGeneratorMapper pTupleKindsToVariablesGeneratorMapping)
	{
		mTupleKindsToVariablesGeneratorMapping = pTupleKindsToVariablesGeneratorMapping;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKinds(Iterable<String> pTupleKinds)
	{
		mTupleKinds = pTupleKinds;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setVarTypeToKernelVariablesLineMapping(
		TypeToKernelVariablesLineMapper pVarTypeToKernelVariablesLineMapping)
	{
		mVarTypeToKernelVariablesLineMapping = pVarTypeToKernelVariablesLineMapping;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setVarTypes(Iterable<String> pVarTypes)
	{
		mVarTypes = pVarTypes;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKindVariableTypeKeyCalculator(
		KernelBuilder.ITupleKindVariableTypeKeyCalculator pVariableSerDeserKeyCalculator)
	{
		mTupleKindVariableTypeKeyCalculator = pVariableSerDeserKeyCalculator;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKindsVarTypesToVariableDeserializationMapping(
		TupleKindsVarTypesToVariableDeserializationMapper pTupleKindsVarTypesToVariableDeserializationMapping)
	{
		mTupleKindsVarTypesToVariableDeserializationMapping = pTupleKindsVarTypesToVariableDeserializationMapping;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKindsVarTypesToVariableSerializationMapping(
		TupleKindsVarTypesToVariableSerializationMapper pTupleKindsVarTypesToVariableSerializationMapping)
	{
		mTupleKindsVarTypesToVariableSerializationMapping = pTupleKindsVarTypesToVariableSerializationMapping;
		return this;
	}
	
	public KernelBuilderOptionsBuilder<T> setTupleKindVarTypeToKernelTypeMapping(TupleKindVarTypeToKernelTypeMapper pTupleKindVarTypeToKernelTypeMapping)
	{
		mTupleKindVarTypeToKernelTypeMapping = pTupleKindVarTypeToKernelTypeMapping;
		return this;
	}
}
