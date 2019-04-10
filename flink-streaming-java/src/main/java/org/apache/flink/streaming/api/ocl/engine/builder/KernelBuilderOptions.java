package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.*;

public class KernelBuilderOptions
{
	private Iterable<String> mUtilityFunctionList;
	private Iterable<String> mKernelParametersList;
	private Iterable<String> mDeserializationMacroList;
	private Iterable<String> mSerializationMacroList;
	private KernelBuilder.IUtilityVariablesGetter mUtilityVariablesGetter;
	
	private TupleKindsToVariablesGeneratorMapper mTupleKindsToVariablesGeneratorMapping;
	private Iterable<String> mTupleKinds;
	
	private TypeToKernelVariablesLineMapper mVarTypeToKernelVariablesLineMapping;
	private Iterable<String> mVarTypes;
	
	private KernelBuilder.ITupleKindVariableTypeKeyCalculator mVariableSerDeserKeyCalculator;
	private TupleKindsVarTypesToVariableDeserializationMapper mTupleKindsVarTypesToVariableDeserializationMapping;
	private TupleKindsVarTypesToVariableSerializationMapper mTupleKindsVarTypesToVariableSerializationMapping;
	
	private TupleKindVarTypeToKernelTypeMapper mTupleKindVarTypeToKernelTypeMapping;
	
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionsRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	public KernelBuilderOptions(
		Iterable<String> pUtilityFunctionList,
		Iterable<String> pKernelParametersList,
		Iterable<String> pDeserializationMacroList,//
		Iterable<String> pSerializationMacroList,//
		KernelBuilder.IUtilityVariablesGetter pUtilityVariablesGetter,
		TupleKindsToVariablesGeneratorMapper pTupleKindsToVariablesGeneratorMapping,
		Iterable<String> pTupleKinds,
		TypeToKernelVariablesLineMapper pVarTypeToKernelVariablesLineMapping,
		Iterable<String> pVarTypes,
		KernelBuilder.ITupleKindVariableTypeKeyCalculator pVariableSerDeserKeyCalculator,
		TupleKindsVarTypesToVariableDeserializationMapper pTupleKindsVarTypesToVariableDeserializationMapping,
		TupleKindsVarTypesToVariableSerializationMapper pTupleKindsVarTypesToVariableSerializationMapping,
		IUserFunction pUserFunction,
		ITupleDefinitionRepository pTupleDefinitionsRepository,
		IOclContextOptions pContextOptions,
		IOclKernelsOptions pKernelOptions,
		TupleKindVarTypeToKernelTypeMapper pTupleKindVarTypeToKernelTypeMapping)
	{
		mUtilityFunctionList = pUtilityFunctionList;
		mKernelParametersList = pKernelParametersList;
		mDeserializationMacroList = pDeserializationMacroList;
		mSerializationMacroList = pSerializationMacroList;
		mUtilityVariablesGetter = pUtilityVariablesGetter;
		mTupleKindsToVariablesGeneratorMapping = pTupleKindsToVariablesGeneratorMapping;
		mTupleKinds = pTupleKinds;
		mVarTypeToKernelVariablesLineMapping = pVarTypeToKernelVariablesLineMapping;
		mVarTypes = pVarTypes;
		mVariableSerDeserKeyCalculator = pVariableSerDeserKeyCalculator;
		mTupleKindsVarTypesToVariableDeserializationMapping = pTupleKindsVarTypesToVariableDeserializationMapping;
		mTupleKindsVarTypesToVariableSerializationMapping = pTupleKindsVarTypesToVariableSerializationMapping;
		mUserFunction = pUserFunction;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mContextOptions = pContextOptions;
		mKernelOptions = pKernelOptions;
		mTupleKindVarTypeToKernelTypeMapping = pTupleKindVarTypeToKernelTypeMapping;
	}
	
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	public ITupleDefinitionRepository getTupleDefinitionsRepository()
	{
		return mTupleDefinitionsRepository;
	}
	
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	
	public IOclKernelsOptions getKernelOptions()
	{
		return mKernelOptions;
	}
	
	public Iterable<String> getUtilityFunctionList()
	{
		return mUtilityFunctionList;
	}
	
	public Iterable<String> getKernelParametersList()
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
	
	public KernelBuilder.IUtilityVariablesGetter getUtilityVariablesGetter()
	{
		return mUtilityVariablesGetter;
	}
	
	public TupleKindsToVariablesGeneratorMapper getTupleKindsToVariablesGeneratorMapping()
	{
		return mTupleKindsToVariablesGeneratorMapping;
	}
	
	public Iterable<String> getTupleKinds()
	{
		return mTupleKinds;
	}
	
	public TypeToKernelVariablesLineMapper getVarTypeToKernelVariablesLineMapping()
	{
		return mVarTypeToKernelVariablesLineMapping;
	}
	
	public Iterable<String> getVarTypes()
	{
		return mVarTypes;
	}
	
	public KernelBuilder.ITupleKindVariableTypeKeyCalculator getVariableSerDeserKeyCalculator()
	{
		return mVariableSerDeserKeyCalculator;
	}
	
	public TupleKindsVarTypesToVariableDeserializationMapper getTupleKindsVarTypesToVariableDeserializationMapping()
	{
		return mTupleKindsVarTypesToVariableDeserializationMapping;
	}
	
	public TupleKindsVarTypesToVariableSerializationMapper getTupleKindsVarTypesToVariableSerializationMapping()
	{
		return mTupleKindsVarTypesToVariableSerializationMapping;
	}
	
	public TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapping()
	{
		return mTupleKindVarTypeToKernelTypeMapping;
	}
	
	/*
	public static class Builder implements IBuilder<KernelBuilderOptions>
	{
		private IUserFunction mUserFunction;
		private ITupleDefinitionRepository mTupleDefinitionsRepository;
		private IOclContextOptions mContextOptions;
		private IOclKernelsOptions mKernelOptions;
		
		private Iterable<String> mUtilityFunctionList;
		private Iterable<String> mKernelParametersList;
		private KernelBuilder.IUtilityVariablesGetter mUtilityVariablesGetter;
		
		private TupleKindsToVariablesGeneratorMapper mTupleKindsToVariablesGeneratorMapping;
		private Iterable<String> mTupleKinds;
		
		private TypeToKernelVariablesLineMapper mVarTypeToKernelVariablesLineMapping;
		private Iterable<String> mVarTypes;
		
		private KernelBuilder.ITupleKindVariableTypeKeyCalculator mVariableSerDeserKeyCalculator;
		private TupleKindsVarTypesToVariableDeserializationMapper mTupleKindsVarTypesToVariableDeserializationMapping;
		private TupleKindsVarTypesToVariableSerializationMapper mTupleKindsVarTypesToVariableSerializationMapping;
		
		private TupleKindVarTypeToKernelTypeMapper mTupleKindVarTypeToKernelTypeMapping;
		
		public Builder(
			IUserFunction pUserFunction,
			ITupleDefinitionRepository pTupleDefinitionsRepository,
			IOclContextOptions pContextOptions,
			IOclKernelsOptions pKernelOptions)
		{
			mUserFunction = pUserFunction;
			mTupleDefinitionsRepository = pTupleDefinitionsRepository;
			mContextOptions = pContextOptions;
			mKernelOptions = pKernelOptions;
		}
		
		public IUserFunction getUserFunction()
		{
			return mUserFunction;
		}
		
		public Builder setUserFunction(IUserFunction pUserFunction)
		{
			mUserFunction = pUserFunction;
			return this;
		}
		
		public ITupleDefinitionRepository getTupleDefinitionRepository()
		{
			return mTupleDefinitionsRepository;
		}
		
		public Builder setTupleDefinitionsRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
		{
			mTupleDefinitionsRepository = pTupleDefinitionsRepository;
			return this;
		}
		
		public IOclContextOptions getContextOptions()
		{
			return mContextOptions;
		}
		
		public Builder setContextOptions(IOclContextOptions pContextOptions)
		{
			mContextOptions = pContextOptions;
			return this;
		}
		
		public IOclKernelsOptions getKernelOptions()
		{
			return mKernelOptions;
		}
		
		public Builder setKernelOptions(IOclKernelsOptions pKernelOptions)
		{
			mKernelOptions = pKernelOptions;
			return this;
		}
		
		public Iterable<String> getUtilityFunctionList()
		{
			return mUtilityFunctionList;
		}
		
		public Builder setUtilityFunctionList(Iterable<String> pUtilityFunctionList)
		{
			mUtilityFunctionList = pUtilityFunctionList;
			return this;
		}
		
		public Iterable<String> getKernelParametersList()
		{
			return mKernelParametersList;
		}
		
		public Builder setKernelParametersList(Iterable<String> pKernelParametersList)
		{
			mKernelParametersList = pKernelParametersList;
			return this;
		}
		
		public KernelBuilder.IUtilityVariablesGetter getUtilityVariablesGetter()
		{
			return mUtilityVariablesGetter;
		}
		
		public Builder setUtilityVariablesGetter(KernelBuilder.IUtilityVariablesGetter pUtilityVariablesGetter)
		{
			mUtilityVariablesGetter = pUtilityVariablesGetter;
			return this;
		}
		
		public TupleKindsToVariablesGeneratorMapper getTupleKindsToVariablesGeneratorMapping()
		{
			return mTupleKindsToVariablesGeneratorMapping;
		}
		
		public Builder setTupleKindsToVariablesGeneratorMapping(
			TupleKindsToVariablesGeneratorMapper pTupleKindsToVariablesGeneratorMapping)
		{
			mTupleKindsToVariablesGeneratorMapping = pTupleKindsToVariablesGeneratorMapping;
			return this;
		}
		
		public Iterable<String> getTupleKinds()
		{
			return mTupleKinds;
		}
		
		public Builder setTupleKinds(Iterable<String> pTupleKinds)
		{
			mTupleKinds = pTupleKinds;
			return this;
		}
		
		public TypeToKernelVariablesLineMapper getVarTypeToKernelVariablesLineMapping()
		{
			return mVarTypeToKernelVariablesLineMapping;
		}
		
		public Builder setVarTypeToKernelVariablesLineMapping(
			TypeToKernelVariablesLineMapper pVarTypeToKernelVariablesLineMapping)
		{
			mVarTypeToKernelVariablesLineMapping = pVarTypeToKernelVariablesLineMapping;
			return this;
		}
		
		public Iterable<String> getVarTypes()
		{
			return mVarTypes;
		}
		
		public Builder setVarTypes(Iterable<String> pVarTypes)
		{
			mVarTypes = pVarTypes;
			return this;
		}
		
		public KernelBuilder.ITupleKindVariableTypeKeyCalculator getTupleKindVariableTypeKeyCalculator()
		{
			return mVariableSerDeserKeyCalculator;
		}
		
		public Builder setTupleKindVariableTypeKeyCalculator(
			KernelBuilder.ITupleKindVariableTypeKeyCalculator pVariableSerDeserKeyCalculator)
		{
			mVariableSerDeserKeyCalculator = pVariableSerDeserKeyCalculator;
			return this;
		}
		
		public TupleKindsVarTypesToVariableDeserializationMapper getTupleKindsVarTypesToVariableDeserializationMapping()
		{
			return mTupleKindsVarTypesToVariableDeserializationMapping;
		}
		
		public Builder setTupleKindsVarTypesToVariableDeserializationMapping(
			TupleKindsVarTypesToVariableDeserializationMapper pTupleKindsVarTypesToVariableDeserializationMapping)
		{
			mTupleKindsVarTypesToVariableDeserializationMapping = pTupleKindsVarTypesToVariableDeserializationMapping;
			return this;
		}
		
		public TupleKindsVarTypesToVariableSerializationMapper getTupleKindsVarTypesToVariableSerializationMapping()
		{
			return mTupleKindsVarTypesToVariableSerializationMapping;
		}
		
		public Builder setTupleKindsVarTypesToVariableSerializationMapping(
			TupleKindsVarTypesToVariableSerializationMapper pTupleKindsVarTypesToVariableSerializationMapping)
		{
			mTupleKindsVarTypesToVariableSerializationMapping = pTupleKindsVarTypesToVariableSerializationMapping;
			return this;
		}
		
		public TupleKindVarTypeToKernelTypeMapper getTupleKindVarTypeToKernelTypeMapping()
		{
			return mTupleKindVarTypeToKernelTypeMapping;
		}
		
		public Builder setTupleKindVarTypeToKernelTypeMapping(TupleKindVarTypeToKernelTypeMapper pTupleKindVarTypeToKernelTypeMapping)
		{
			mTupleKindVarTypeToKernelTypeMapping = pTupleKindVarTypeToKernelTypeMapping;
			return this;
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
				getTupleDefinitionRepository(),
				getContextOptions(),
				getKernelOptions(),
				getTupleKindVarTypeToKernelTypeMapping()
			);
		}
	}
	*/
}
