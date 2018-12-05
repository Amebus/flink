package org.apache.flink.api.newEngine.kernel.builder;

import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.kernel.OclKernel;
import org.apache.flink.api.newEngine.kernel.builder.mappers.*;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.newConfiguration.ITupleDefinitionRepository;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class KernelBuilder implements IBuilder<OclKernel>
{
	private IUserFunction mUserFunction;
	private ITupleDefinitionRepository mTupleDefinitions;
	private IOclContextOptions mOclContextOptions;
	private IOclKernelsOptions mOclKernelOptions;
	
	private Iterable<String> mUtilityFunctionList;
	private Iterable<String> mKernelParametersList;
	private IUtilityVariablesGetter mUtilityVariablesGetter;
	
	//Key -> tupleKinds
	private TupleKindsToVariablesGeneratorMapper mTupleKindsToVariablesGeneratorMapping;
	private Iterable<String> mTupleKinds;
	
	//Key -> varTypes
	private Iterable<String> mVarTypes;
	
	//Key -> tupleKinds + varTypes generated with ITupleKindVariableTypeKeyCalculator
	private ITupleKindVariableTypeKeyCalculator mVariableSerDeserKeyCalculator;
	private TypeToKernelVariablesLineMapper mVarTypeToKernelVariablesLineMapping;
	private TupleKindsVarTypesToVariableDeserializationMapper mTupleKindsVarTypesToVariableDeserializationMapping;
	private TupleKindsVarTypesToVariableSerializationMapper mTupleKindsVarTypesToVariableSerializationMapping;
	
	//
	private TupleKindVarTypeToKernelTypeMapper mTupleKindVarTypeToKernelTypeMapping;
	
	public KernelBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		mUserFunction = pKernelBuilderOptions.getUserFunction();
		mTupleDefinitions = pKernelBuilderOptions.getTupleDefinitionsRepository();
		mOclContextOptions = pKernelBuilderOptions.getContextOptions();
		mOclKernelOptions = pKernelBuilderOptions.getKernelOptions();
		
		
		mUtilityFunctionList = pKernelBuilderOptions.getUtilityFunctionList();
		mKernelParametersList = pKernelBuilderOptions.getKernelParametersList();
		mUtilityVariablesGetter = pKernelBuilderOptions.getUtilityVariablesGetter();
		
		mTupleKindsToVariablesGeneratorMapping = pKernelBuilderOptions.getTupleKindsToVariablesGeneratorMapping();
		mTupleKinds = pKernelBuilderOptions.getTupleKinds();
		
		mVarTypes = pKernelBuilderOptions.getVarTypes();
		
		mVariableSerDeserKeyCalculator = pKernelBuilderOptions.getVariableSerDeserKeyCalculator();
		mVarTypeToKernelVariablesLineMapping = pKernelBuilderOptions.getVarTypeToKernelVariablesLineMapping();
		mTupleKindsVarTypesToVariableDeserializationMapping =
			pKernelBuilderOptions.getTupleKindsVarTypesToVariableDeserializationMapping();
		mTupleKindsVarTypesToVariableSerializationMapping =
			pKernelBuilderOptions.getTupleKindsVarTypesToVariableSerializationMapping();
		
		mTupleKindVarTypeToKernelTypeMapping = pKernelBuilderOptions.getTupleKindVarTypeToKernelTypeMapping();
	}
	
	public IUserFunction getUserFunction()
	{
		return mUserFunction;
	}
	
	public ITupleDefinitionRepository getTupleDefinitions()
	{
		return mTupleDefinitions;
	}
	
	public IOclContextOptions getOclContextOptions()
	{
		return mOclContextOptions;
	}
	
	public IOclKernelsOptions getOclKernelOptions()
	{
		return mOclKernelOptions;
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
	
	public ITupleKindVariableTypeKeyCalculator getVariableSerDeserKeyCalculator()
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
	
	@Override
	public OclKernel build()
	{
		return new OclKernel(getKernelName(), getKernelCode());
	}
	
	protected String getKernelName()
	{
		return getUserFunction().getName();
	}
	
	protected String getKernelCode()
	{
		HashMap<String, Iterable<KernelLogicalVariable>> vKernelLogicalVariables
			= translateVariablesToKernelLogicalVariables();
		
		HashMap<String, KernelVariablesLine> vKernelVariablesLines
			= getKernelVariablesLines(vKernelLogicalVariables);
		
		return getUtilityFunctions() + "\n" +
			   getSerializationMacros() + "\n" +
			   getDeserializationMacros() + "\n" +
			   getKernelSignature() + "\n" +
			   "\n{\n" +
			   getUtilityVars() +
			   getVariableDefinitionsLine(vKernelVariablesLines) +
			   getDeserialization(vKernelLogicalVariables) +
			   getUserFunction().getFunction() + "\n" +
			   getSerialization(vKernelLogicalVariables) +
			   "\n};\n";
	}
	
	protected String getUtilityFunctions()
	{
		StringBuilder vBuilder = new StringBuilder(100000);
		mUtilityFunctionList.forEach(vBuilder::append);
		return vBuilder.toString();
	}
	
	protected String getSerializationMacros()
	{
		return "";
	}
	
	protected String getDeserializationMacros()
	{
		return "";
	}
	
	protected String getKernelSignature()
	{
		StringBuilder vBuilder =
			new StringBuilder(200)
			.append("(\n");
		
		Iterator<String> vIterator = mKernelParametersList.iterator();
		while (vIterator.hasNext())
		{
			vBuilder
				.append("\t")
				.append(vIterator.next());
			
			if(vIterator.hasNext())
			{
				vBuilder.append(",\n");
			}
		}
		
		vBuilder.append(")\n");
		
		return "__kernel void " +
			   getKernelName() +
			   vBuilder.toString();
	}
	
	protected String getUtilityVars()
	{
		return "int _gId = get_global_id(0);\n" +
			   mUtilityVariablesGetter.getUtilityVariables() +
			   "\n";
	}
	
	protected String getVariableDefinitionsLine(HashMap<String, KernelVariablesLine> pKernelVariablesLines)
	{
		StringBuilder vBuilder = new StringBuilder(100000);
		
		forEachTupleKind(pTupleKind ->
						 {
			
							 forEachVarType(pVarType ->
											{
												String vKey =
													mVariableSerDeserKeyCalculator
														.getKey(pTupleKind, pVarType);
				
												KernelVariablesLine vVariablesLine =
													pKernelVariablesLines.get(vKey);
				
												vBuilder
													.append(mTupleKindVarTypeToKernelTypeMapping.resolve(vKey))
													.append(" ");
				
												Iterator<String> vIterator = vVariablesLine.getVarDefinition().iterator();
												String vVarDef;
				
												while (vIterator.hasNext())
												{
													vVarDef = vIterator.next();
					
													vBuilder.append(vVarDef);
					
													if(vIterator.hasNext())
													{
														vBuilder.append(",");
													}
													else
													{
														vBuilder.append(";");
													}
													vBuilder.append("\n");
												}
											});
							 vBuilder
								 .append("//")
								 .append(pTupleKind)
								 .append("\n");
						 });
		
		return vBuilder.toString();
	}
	
	protected HashMap<String, Iterable<KernelLogicalVariable>> translateVariablesToKernelLogicalVariables()
	{
		HashMap<String, Iterable<KernelLogicalVariable>> vResult = new HashMap<>();
		IUserFunction vUserFunction = getUserFunction();
		ITupleDefinitionRepository vRepository = getTupleDefinitions();
		
		forEachTupleKind(pTupleKind ->
						 {
							 Iterable<KernelLogicalVariable> vVariables =
								 mTupleKindsToVariablesGeneratorMapping
									 .resolve(pTupleKind)
									 .getKernelLogicalVariables(vUserFunction, vRepository);
			
							 vResult.put(pTupleKind, vVariables);
							 //vVariables.forEach(pKernelLogicalVariable -> vResult.put(pTupleKind, vVariables));
						 });
		
		return vResult;
	}
	
	protected HashMap<String, KernelVariablesLine> getKernelVariablesLines(
		HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		HashMap<String, KernelVariablesLine> vResult = new HashMap<>();
		
		forEachTupleKind(pTupleKind ->
						 {
						 	
							 Iterable<KernelLogicalVariable> vVariables = pKernelLogicalVariables.get(pTupleKind);
							 
							 if(vVariables == null || !vVariables.iterator().hasNext())
							 {
							 	return;
							 }
							 
							 forEachVarType( pVarType ->
											 {
											 	String vKey = mVariableSerDeserKeyCalculator
													.getKey(pTupleKind, pVarType);
											 		vResult
														.put(vKey,
															 mVarTypeToKernelVariablesLineMapping
																 .resolve(vKey)
																 .getKernelVariablesLine(StreamUtility
																							 .streamFrom(vVariables)
																							 .filter(pVariable -> pVarType
																								 .equals(pVariable.getVarType()))
																							 .collect(Collectors.toList())));
											 });
						 });
		
		return vResult;
	}
	
	protected String getDeserialization(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		StringKeyMapper<IVariableDeserialization> vMapper = mTupleKindsVarTypesToVariableDeserializationMapping;
		
		if(vMapper.isEmpty())
		{
			return "//Empty Deserialization routine\n";
		}
		
		StringBuilder vBuilder = new StringBuilder(100000);
		forEachTupleKind(pTupleKind ->
						 {
							 Iterable<KernelLogicalVariable> vVariables = pKernelLogicalVariables.get(pTupleKind);
							 List<KernelDeserializationLine> vLines = new ArrayList<>();
							 
						 	 forEachVarType(pVarType ->
										   {
											   IVariableDeserialization vVarDeser;
											   String vKey = mVariableSerDeserKeyCalculator.getKey(pTupleKind, pVarType);
											   Iterable<KernelLogicalVariable> vIter =
												   StreamUtility
												   .streamFrom(vVariables)
												   .filter(pVariable -> pVarType
													   .equals(pVariable.getVarType()))
												   .collect(Collectors.toList());
											   if(vMapper.containsKey(vKey))
											   {
												   vVarDeser = vMapper.resolve(vKey);
												   vIter
													   .forEach(pLogicVar ->
																	vLines.add(
																		vVarDeser.getKernelDeserializationLine(pLogicVar)));
											   }
										   });
						 	 
						 	 vLines.sort((o1, o2) ->
										 {
											 int vResult = 0;
											 if(o1.getDeserIndexOrder() > o2.getDeserIndexOrder())
											 {
											 	vResult = 1;
											 }
											 else if(o1.getDeserIndexOrder() < o2.getDeserIndexOrder())
											 {
											 	vResult = -1;
											 }
											 return vResult;
										 });
						 	 
						 	 vLines.forEach(pLine -> vBuilder.append(pLine).append("\n"));
							 
						 	 vBuilder.append("\n");
						 });
		return vBuilder.toString();
	}
	
	protected String getSerialization(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		StringKeyMapper<IVariableSerialization> vMapper = mTupleKindsVarTypesToVariableSerializationMapping;
		
		if(vMapper.isEmpty())
		{
			return "//Empty Serialization routine\n";
		}
		
		StringBuilder vBuilder = new StringBuilder(100000);
		forEachTupleKind(pTupleKind ->
						 {
							 Iterable<KernelLogicalVariable> vVariables = pKernelLogicalVariables.get(pTupleKind);
							 List<KernelSerializationLine> vLines = new ArrayList<>();
			
							 forEachVarType(pVarType ->
											{
												IVariableSerialization vVarSer;
												String vKey = mVariableSerDeserKeyCalculator.getKey(pTupleKind, pVarType);
												Iterable<KernelLogicalVariable> vIter =
													StreamUtility
														.streamFrom(vVariables)
														.filter(pVariable -> pVarType
															.equals(pVariable.getVarType()))
														.collect(Collectors.toList());
												if(vMapper.containsKey(vKey))
												{
													vVarSer = vMapper.resolve(vKey);
													vIter
														.forEach(pLogicVar ->
																	 vLines.add(
																		 vVarSer.getKernelSerializationLine(pLogicVar)));
												}
											});
			
							 vLines.sort((o1, o2) ->
										 {
											 int vResult = 0;
											 if(o1.getSerIndexOrder() > o2.getSerIndexOrder())
											 {
												 vResult = 1;
											 }
											 else if(o1.getSerIndexOrder() < o2.getSerIndexOrder())
											 {
												 vResult = -1;
											 }
											 return vResult;
										 });
			
							 vLines.forEach(pLine -> vBuilder.append(pLine).append("\n"));
			
							 vBuilder.append("\n");
						 });
		return vBuilder.toString();
	}
	
	private void forEachTupleKind(Consumer<String> action)
	{
		getTupleKinds().forEach(action);
	}
	
	private void forEachVarType(Consumer<String> action)
	{
		getVarTypes().forEach(action);
	}
	
	@FunctionalInterface
	public interface IUtilityVariablesGetter
	{
		String getUtilityVariables();
	}
	
	/**
	 * uno per TupleKind
	 */
	@FunctionalInterface
	public interface IKernelVariablesGenerator
	{
		Iterable<KernelLogicalVariable> getKernelLogicalVariables(
			IUserFunction pUserFunction,
			ITupleDefinitionRepository pTupleDefinitionRepository);
	}
	
	/**
	 * uno per KernelLogicalVariable.getVarType()
	 */
	@FunctionalInterface
	public interface IKernelVariablesLineGenerator
	{
		KernelVariablesLine getKernelVariablesLine(Iterable<KernelLogicalVariable> pKernelLogicalVariables);
	}
	
	@FunctionalInterface
	public interface ITupleKindVariableTypeKeyCalculator
	{
		String getKey(String pTupleKinds, String pVarType);
	}
	
	/**
	 * uno per KernelLogicalVariable.getVarType()
	 * da eseguire per ogni KernelLogicalVariable
	 *
	 * uno per TupleKind
	 *
	 */
	@FunctionalInterface
	public interface IVariableDeserialization
	{
		KernelDeserializationLine getKernelDeserializationLine(KernelLogicalVariable pKernelLogicalVariable);
	}
	
	/**
	 * uno per KernelLogicalVariable.getVarType()
	 * da eseguire per ogni KernelLogicalVariable
	 *
	 * uno per TupleKind
	 *
	 */
	@FunctionalInterface
	public interface IVariableSerialization
	{
		KernelSerializationLine getKernelSerializationLine(KernelLogicalVariable pKernelLogicalVariable);
	}
	
	public static class KernelLogicalVariable
	{
		private String mVarType;
		private String mVarName;
		private int mIndex;
		
		public KernelLogicalVariable(String pVarType, String pVarName, int pIndex)
		{
			mVarType = pVarType;
			mVarName = pVarName;
			mIndex = pIndex;
		}
		
		public String getVarType()
		{
			return mVarType;
		}
		
		public String getVarName()
		{
			return mVarName;
		}
		
		public int getIndex()
		{
			return mIndex;
		}
	}
	
	public static class KernelVariablesLine
	{
		private String mVarType;
		private List<String> mVarDefinition;
		
		public KernelVariablesLine(String pVarType)
		{
			mVarType = pVarType;
			mVarDefinition = new LinkedList<>();
		}
		
		public String getVarType()
		{
			return mVarType;
		}
		
		public Iterable<String> getVarDefinition()
		{
			return mVarDefinition;
		}
		
		public KernelVariablesLine setVarType(String pVarType)
		{
			mVarType = pVarType;
			return this;
		}
		
		public KernelVariablesLine addVarNames(String pVarName)
		{
			mVarDefinition.add(pVarName);
			return this;
		}
	}
	
	public static class KernelDeserializationLine
	{
		private String mDeserLine;
		private int mDeserIndexOrder;
		
		public KernelDeserializationLine(String pDeserLine, int pDeserIndexOrder)
		{
			mDeserLine = pDeserLine;
			mDeserIndexOrder = pDeserIndexOrder;
		}
		
		public String getDeserLine()
		{
			return mDeserLine;
		}
		
		public int getDeserIndexOrder()
		{
			return mDeserIndexOrder;
		}
	}
	
	public static class KernelSerializationLine
	{
		private String mSerLine;
		private int mSerIndexOrder;
		
		public KernelSerializationLine(String pSerLine, int pSerIndexOrder)
		{
			mSerLine = pSerLine;
			mSerIndexOrder = pSerIndexOrder;
		}
		
		public String getSerLine()
		{
			return mSerLine;
		}
		
		public int getSerIndexOrder()
		{
			return mSerIndexOrder;
		}
	}
}
