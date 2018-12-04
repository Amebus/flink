package org.apache.flink.api.newEngine.kernel.builder;

import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.newEngine.kernel.builder.mappers.TupleKindsToVariablesGeneratorMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.TupleKindsVarTypesToVariableDeserializationMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.TupleKindsVarTypesToVariableSerializationMapper;
import org.apache.flink.api.newEngine.kernel.builder.mappers.TypeToKernelVariablesLineMapper;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.newConfiguration.ITupleDefinitionRepository;

public class KernelBuilderOptions
{
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
	
	
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitionsRepository;
	private IOclContextOptions mContextOptions;
	private IOclKernelsOptions mKernelOptions;
	
	public KernelBuilderOptions(
		Iterable<String> pUtilityFunctionList,
		Iterable<String> pKernelParametersList,
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
		IOclKernelsOptions pKernelOptions)
	{
		mUtilityFunctionList = pUtilityFunctionList;
		mKernelParametersList = pKernelParametersList;
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
	
	public static class KernelOptionsBuilder implements IBuilder<KernelBuilderOptions>
	{
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
		
		private IUserFunction mUserFunction;
		private ITupleDefinitionRepository mTupleDefinitionsRepository;
		private IOclContextOptions mContextOptions;
		private IOclKernelsOptions mKernelOptions;
		
		public KernelOptionsBuilder setUserFunction(IUserFunction pUserFunction)
		{
			mUserFunction = pUserFunction;
			return this;
		}
		
		public KernelOptionsBuilder setTupleDefinitionRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
		{
			mTupleDefinitionsRepository = pTupleDefinitionsRepository;
			return this;
		}
		
		public KernelOptionsBuilder setContextOptions(IOclContextOptions pContextOptions)
		{
			mContextOptions = pContextOptions;
			return this;
		}
		
		public KernelOptionsBuilder setKernelOptions(IOclKernelsOptions pKernelOptions)
		{
			mKernelOptions = pKernelOptions;
			return this;
		}
		
		public KernelOptionsBuilder setUtilityFunctionList(Iterable<String> pUtilityFunctionList)
		{
			mUtilityFunctionList = pUtilityFunctionList;
			return this;
		}
		
		public KernelOptionsBuilder setKernelParametersList(Iterable<String> pKernelParametersList)
		{
			mKernelParametersList = pKernelParametersList;
			return this;
		}
		
		public KernelOptionsBuilder setUtilityVariablesGetter(KernelBuilder.IUtilityVariablesGetter pUtilityVariablesGetter)
		{
			mUtilityVariablesGetter = pUtilityVariablesGetter;
			return this;
		}
		
		public KernelOptionsBuilder setTupleKindsToVariablesGeneratorMapping(TupleKindsToVariablesGeneratorMapper pTupleKindsToVariablesGeneratorMapping)
		{
			mTupleKindsToVariablesGeneratorMapping = pTupleKindsToVariablesGeneratorMapping;
			return this;
		}
		
		public KernelOptionsBuilder setTupleKinds(Iterable<String> pTupleKinds)
		{
			mTupleKinds = pTupleKinds;
			return this;
		}
		
		public KernelOptionsBuilder setVarTypeToKernelVariablesLineMapping(TypeToKernelVariablesLineMapper pVarTypeToKernelVariablesLineMapping)
		{
			mVarTypeToKernelVariablesLineMapping = pVarTypeToKernelVariablesLineMapping;
			return this;
		}
		
		public KernelOptionsBuilder setVarTypes(Iterable<String> pVarTypes)
		{
			mVarTypes = pVarTypes;
			return this;
		}
		
		public KernelOptionsBuilder setVariableSerDeserKeyCalculator(KernelBuilder.ITupleKindVariableTypeKeyCalculator pVariableSerDeserKeyCalculator)
		{
			mVariableSerDeserKeyCalculator = pVariableSerDeserKeyCalculator;
			return this;
		}
		
		public KernelOptionsBuilder setTupleKindsVarTypesToVariableDeserializationMapping(TupleKindsVarTypesToVariableDeserializationMapper pTupleKindsVarTypesToVariableDeserializationMapping)
		{
			mTupleKindsVarTypesToVariableDeserializationMapping = pTupleKindsVarTypesToVariableDeserializationMapping;
			return this;
		}
		
		public KernelOptionsBuilder setTupleKindsVarTypesToVariableSerializationMapping(TupleKindsVarTypesToVariableSerializationMapper pTupleKindsVarTypesToVariableSerializationMapping)
		{
			mTupleKindsVarTypesToVariableSerializationMapping = pTupleKindsVarTypesToVariableSerializationMapping;
			return this;
		}
		
		public KernelOptionsBuilder setTupleDefinitionsRepository(ITupleDefinitionRepository pTupleDefinitionsRepository)
		{
			mTupleDefinitionsRepository = pTupleDefinitionsRepository;
			return this;
		}
		
		@Override
		public KernelBuilderOptions build()
		{
			return new KernelBuilderOptions(
				mUtilityFunctionList,
				mKernelParametersList,
				mUtilityVariablesGetter,
				mTupleKindsToVariablesGeneratorMapping,
				mTupleKinds,
				mVarTypeToKernelVariablesLineMapping,
				mVarTypes,
				mVariableSerDeserKeyCalculator,
				mTupleKindsVarTypesToVariableDeserializationMapping,
				mTupleKindsVarTypesToVariableSerializationMapping,
				mUserFunction,
				mTupleDefinitionsRepository,
				mContextOptions,
				mKernelOptions
			);
		}
	}
}
