package org.apache.flink.streaming.api.ocl.configuration;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;
import org.apache.flink.streaming.api.ocl.common.IBuilder;
import org.apache.flink.streaming.api.ocl.common.IMapperKeyComparerWrapper;
import org.apache.flink.streaming.api.ocl.common.JsonLoader;
import org.apache.flink.streaming.api.ocl.common.JsonLoaderOptions;
import org.apache.flink.streaming.api.ocl.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;
import org.apache.flink.streaming.configuration.ITupleVarDefinition;
import org.apache.flink.streaming.configuration.tuple.TupleDefinition;
import org.apache.flink.streaming.configuration.tuple.TupleVarDefinition;

import java.util.*;

public class JsonTupleRepository implements ITupleDefinitionRepository
{
	public static final int TUPLE_SUPPORTED_MAX_ARITY = 22;
	public static final int TUPLE_SUPPORTED_MIN_ARITY = 0;
	
	private boolean mTupleDefinitionsLoaded;
	private String mFileDirectory;
	private String mFileName;
	private String mTupleVarOptionsSeparatorSequenceRegex;
	
	private Map<String, ITupleDefinition> mTupleDefinitions;
	
	private StringKeyMapper<ITupleVariableExtractor> mTupleVariableExtractors;
	private Iterable<String> mTupleSupportedTypes;
	private Iterable<String> mTupleEngineTypes;
	
	protected JsonTupleRepository(String pFileDirectory, String pFileName, String pTupleVarOptionsSeparatorSequenceRegex)
	{
		mTupleDefinitions = new HashMap<>();
		mFileDirectory = pFileDirectory;
		mFileName = pFileName;
		mTupleVarOptionsSeparatorSequenceRegex = pTupleVarOptionsSeparatorSequenceRegex;
		
		setUpVariableExtractors();
	}
	
	protected void setUpVariableExtractors()
	{
		setUpVariableExtractors(new StringKeyCaseInsenstiveComparer(""));
	}
	
	protected void setUpVariableExtractors(IMapperKeyComparerWrapper<String> pKeyComparerWrapper)
	{
		mTupleVariableExtractors = new StringKeyMapper<>(pKeyComparerWrapper);
		Iterable<ITupleVariableExtractor> vExtractorList = getVariableExtractorList();
		
		setTupleVariableExtractors(vExtractorList);
		setTupleSupportedTypes(vExtractorList);
		setTupleEngineTypes(vExtractorList);
	}
	
	protected Iterable<ITupleVariableExtractor> getVariableExtractorList()
	{
		List<ITupleVariableExtractor> vExtractorList = new ArrayList<>(3);
		vExtractorList.add(new IntegerTupleVariableExtractor());
		vExtractorList.add(new DoubleTupleVariableExtractor());
		vExtractorList.add(new StringTupleVariableExtractor());
		return vExtractorList;
	}
	
	protected void setTupleVariableExtractors(Iterable<ITupleVariableExtractor> pVariableExtractors)
	{
		pVariableExtractors.forEach(vExtractor ->
										vExtractor.getRecognizedNameTypes()
												  .forEach(v -> mTupleVariableExtractors.register(v, vExtractor)));
	}
	
	protected void setTupleSupportedTypes(Iterable<ITupleVariableExtractor> pVariableExtractors)
	{
		List<String> vTupleSupportedTypes = new ArrayList<>();
		pVariableExtractors.forEach(vExtractor -> vExtractor.getRecognizedNameTypes().forEach(vTupleSupportedTypes::add));
		mTupleSupportedTypes = vTupleSupportedTypes;
	}
	
	protected void setTupleEngineTypes(Iterable<ITupleVariableExtractor> pVariableExtractors)
	{
		List<String> vTupleEngineTypes = new ArrayList<>();
		pVariableExtractors.forEach(vExtractor -> vTupleEngineTypes.add(vExtractor.getTupleEngineType()));
		mTupleEngineTypes = vTupleEngineTypes;
	}
	
	@SuppressWarnings("unchecked")
	private void loadTupleDefinitions()
	{
		if(mTupleDefinitionsLoaded)
			return;
		
		Map<String, Map<String, String>> vMap;
		Object vObject = JsonLoader.loadJsonObject(new JsonLoaderOptions.JsonLoaderOptionsBuilder<>()
													   .setSource(mFileDirectory, mFileName)
													   .setBeanClass(Object.class)
													   .build());
		
		vMap = (Map<String, Map<String, String>>)vObject;
		
		mTupleDefinitions = new HashMap<>();
		vMap.forEach( (k, v) -> mTupleDefinitions.put(k, createTupleDefinitionFromJson(k, v)));
		
		mTupleDefinitionsLoaded = true;
	}
	
	protected ITupleDefinition createTupleDefinitionFromJson(String pName, Map<String, String> pTypesMap)
	{
		Collection<ITupleVarDefinition> pTupleVarDefinitions;
		
		final RegularExpression vExpression = new RegularExpression("t\\d+");
		Set<String> vKeySet = new HashSet<>(pTypesMap.keySet());
		
		System.out.println("Parsing tuple " + pName + " ...");
		
		vKeySet.forEach( x ->
						 {
							 if(vExpression.matches(x))
							 {
								 String vTempString = x.substring(1);
								 int vValue = Integer.parseInt(vTempString);
								 if( getTupleSupportedMinArity() <= vValue && vValue < getTupleSupportedMaxArity())
								 {
									 return;
								 }
							 }
							 System.out.println("Tuple skipped because exceeds the supported arity limits " +
												"[" + getTupleSupportedMinArity() + "," +
												"" + getTupleSupportedMaxArity() + ")");
							 pTypesMap.remove(x);
							 
							 System.out.println();
							 
						 });
		
		pTupleVarDefinitions = new ArrayList<>(pTypesMap.size());
		
		final int[] vIndex = {0};
		pTypesMap.forEach( (k,v) ->
						   {
							   String[] vChunks = v.split(mTupleVarOptionsSeparatorSequenceRegex);
							   ITupleVariableExtractor vExtractor = mTupleVariableExtractors
								   .resolve(vChunks[0]);
							   pTupleVarDefinitions.add(vExtractor
															.getTupleVarDefinition(vChunks, vIndex[0]));
							   vIndex[0]++;
						   });
		
		return new TupleDefinition(pName, pTupleVarDefinitions);
	}
	
	@Override
	public int getTupleSupportedMaxArity()
	{
		return TUPLE_SUPPORTED_MAX_ARITY;
	}
	@Override
	public int getTupleSupportedMinArity()
	{
		return TUPLE_SUPPORTED_MIN_ARITY;
	}
	
	@Override
	public Iterable<String> getTupleSupportedTypes()
	{
		return mTupleSupportedTypes;
	}
	@Override
	public Iterable<String> getTupleEngineTypes()
	{
		return mTupleEngineTypes;
	}
	
	@Override
	public Iterable<ITupleDefinition> getTupleDefinitions()
	{
		loadTupleDefinitions();
		return mTupleDefinitions.values();
	}
	
	@Override
	public ITupleDefinition getTupleDefinition(String pTupleName)
	{
		loadTupleDefinitions();
		return mTupleDefinitions.get(pTupleName);
	}
	
	
	public static class Builder implements IBuilder<JsonTupleRepository>
	{
		public static final String DEFAULT_FILE_NAME = "tuples.json";
		public static final String DEFAULT_TUPLE_VAR_OPTIONS_SEPARATOR_SEQUENCE = "::";
		
		private String mFileDirectory;
		private String mFileName;
		private String mTupleVarOptionsSeparatorSequence;
		
		protected String getDefaultFileName()
		{
			return DEFAULT_FILE_NAME;
		}
		protected String getDefaultTupleVarOptionsSeparatorSequence()
		{
			return DEFAULT_TUPLE_VAR_OPTIONS_SEPARATOR_SEQUENCE;
		}
		
		public Builder(String pFileDirectory)
		{
			mFileDirectory = pFileDirectory;
			mFileName = getDefaultFileName();
			mTupleVarOptionsSeparatorSequence = getDefaultTupleVarOptionsSeparatorSequence();
		}
		
		public Builder setFileDirectory(String pFileDirectory)
		{
			mFileDirectory = pFileDirectory;
			return this;
		}
		
		public Builder setFileName(String pFileName)
		{
			mFileName = pFileName;
			return this;
		}
		
		public Builder setTupleVarOptionsSeparatorSequence(String pTupleVarOptionsSeparatorSequence)
		{
			mTupleVarOptionsSeparatorSequence = pTupleVarOptionsSeparatorSequence;
			return this;
		}
		
		@Override
		public JsonTupleRepository build()
		{
			return new JsonTupleRepository(mFileDirectory, mFileName, mTupleVarOptionsSeparatorSequence);
		}
	}
	
	public interface ITupleVariableExtractor
	{
		ITupleVarDefinition getTupleVarDefinition(String[] pVarDefinitionChunks, int pIndex);
		
		Iterable<String> getRecognizedNameTypes();
		
		String getTupleEngineType();
	}
	
	public abstract class GenericTupleVariableExtractor implements ITupleVariableExtractor
	{
		private List<String> mRecognizedTypes;
		
		protected GenericTupleVariableExtractor(int pRecognizedTypesCount)
		{
			mRecognizedTypes = new ArrayList<>(pRecognizedTypesCount);
		}
		
		protected void addRecognizedType(String pRecognizedType)
		{
			mRecognizedTypes.add(pRecognizedType);
		}
		
		@Override
		public Iterable<String> getRecognizedNameTypes()
		{
			return mRecognizedTypes;
		}
		
		abstract int getDefaultMaxReservedBytes();
	}
	
	public class IntegerTupleVariableExtractor extends GenericTupleVariableExtractor
	{
		public static final String INT_TYPE = "int";
		public static final String INTEGER_TYPE = "integer";
		
		public static final int DEFAULT_MAX_RESERVED_BYTES = 4;
		
		public IntegerTupleVariableExtractor()
		{
			super(2);
			addRecognizedType(INT_TYPE);
			addRecognizedType(INTEGER_TYPE);
		}
		
		@Override
		public ITupleVarDefinition getTupleVarDefinition(String[] pVarDefinitionChunks, int pIndex)
		{
			Integer vIdentityValue = null;
			
			if(pVarDefinitionChunks.length > 1 && pVarDefinitionChunks[1].startsWith("I"))
			{
				vIdentityValue = Integer.valueOf(pVarDefinitionChunks[1].substring(1));
			}
			
			return new TupleVarDefinition(
				getTupleEngineType(),
				getDefaultMaxReservedBytes(),
				vIdentityValue,
				pIndex);
		}
		
		@Override
		public String getTupleEngineType()
		{
			return INT_TYPE;
		}
		
		@Override
		public int getDefaultMaxReservedBytes()
		{
			return DEFAULT_MAX_RESERVED_BYTES;
		}
	}
	
	public class DoubleTupleVariableExtractor extends GenericTupleVariableExtractor
	{
		public static final String DOUBLE_TYPE = "double";
		
		public static final int DEFAULT_MAX_RESERVED_BYTES = 8;
		
		public DoubleTupleVariableExtractor()
		{
			super(1);
			addRecognizedType(DOUBLE_TYPE);
		}
		
		@Override
		public ITupleVarDefinition getTupleVarDefinition(String[] pVarDefinitionChunks, int pIndex)
		{
			Double vIdentityValue = null;
			
			if(pVarDefinitionChunks.length > 1 && pVarDefinitionChunks[1].startsWith("I"))
			{
				vIdentityValue = Double.valueOf(pVarDefinitionChunks[1].substring(1));
			}
			
			return new TupleVarDefinition(
				getTupleEngineType(),
				getDefaultMaxReservedBytes(),
				vIdentityValue,
				pIndex);
		}
		
		@Override
		public String getTupleEngineType()
		{
			return DOUBLE_TYPE;
		}
		
		@Override
		public int getDefaultMaxReservedBytes()
		{
			return DEFAULT_MAX_RESERVED_BYTES;
		}
	}
	
	public class StringTupleVariableExtractor extends GenericTupleVariableExtractor
	{
		
		public static final String STRING_TYPE = "string";
		public static final String CHAR_ARRAY_TYPE = "char[]";
		public static final String CHAR_POINTER_TYPE = "char *";
		public static final String CHAR_POINTER_TYPE_2 = "char*";
		
		public static final int DEFAULT_MAX_RESERVED_BYTES = 100;
		
		protected StringTupleVariableExtractor()
		{
			super(4);
			addRecognizedType(STRING_TYPE);
			addRecognizedType(CHAR_ARRAY_TYPE);
			addRecognizedType(CHAR_POINTER_TYPE);
			addRecognizedType(CHAR_POINTER_TYPE_2);
		}
		
		@Override
		public ITupleVarDefinition getTupleVarDefinition(String[] pVarDefinitionChunks, int pIndex)
		{
			String vOpt;
			String vIdentityValue = null;
			int vMaxReservedBytes = getDefaultMaxReservedBytes();
			
			for (int vI = 1; vI < pVarDefinitionChunks.length; vI++)
			{
				vOpt = pVarDefinitionChunks[vI];
				
				if(vOpt.startsWith("D"))
				{
					vMaxReservedBytes = Integer.valueOf(vOpt.substring(1));
				}
				else if(vOpt.startsWith("I"))
				{
					vIdentityValue = vOpt.substring(1);
				}
			}
			
			return new TupleVarDefinition(
				getTupleEngineType(),
				vMaxReservedBytes,
				vIdentityValue,
				pIndex);
		}
		
		@Override
		public String getTupleEngineType()
		{
			return STRING_TYPE;
		}
		
		@Override
		public int getDefaultMaxReservedBytes()
		{
			return DEFAULT_MAX_RESERVED_BYTES;
		}
	}
}
