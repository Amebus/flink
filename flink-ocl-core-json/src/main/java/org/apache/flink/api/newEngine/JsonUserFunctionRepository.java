package org.apache.flink.api.newEngine;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.flink.api.common.IBuilder;
import org.apache.flink.api.common.JsonLoader;
import org.apache.flink.api.common.JsonLoaderOptions;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JsonUserFunctionRepository //implements IUserFunctionsRepository
{
	private static final int SUPPORTED_FUNCTION_TYPES_COUNT = 3;
	
	//Transformations
	public static final String MAP = "map";
	public static final String FILTER = "filter";
	
	//Actions
	public static final String REDUCE = "reduce";
	
	private Map<String, IUserFunction> mUserFunctionMap;
	
	private String mFileDirectory;
	private String mFileName;
	
	private boolean mAreFunctionsNotLoadedYet;
	
	private Iterable<String> mFunctionEngineTypes;
	
	protected JsonUserFunctionRepository(String pFileDirectory, String pFileName)
	{
		mAreFunctionsNotLoadedYet = true;
		mUserFunctionMap = new HashMap<>();
		mFileDirectory = pFileDirectory;
		mFileName = pFileName;
		setFunctionEngineTypes();
	}
	
	protected int getSupportedFunctionTypesCount()
	{
		return SUPPORTED_FUNCTION_TYPES_COUNT;
	}
	
	protected void setFunctionEngineTypes()
	{
		setFunctionEngineTypes(getSupportedFunctionTypesCount());
	}
	
	protected void setFunctionEngineTypes(int pSupportedFunctionTypesCount)
	{
		List<String> vFunctionEngineTypes = new ArrayList<>(pSupportedFunctionTypesCount);
		
		vFunctionEngineTypes.add(FILTER);
		vFunctionEngineTypes.add(MAP);
		vFunctionEngineTypes.add(REDUCE);
		
		mFunctionEngineTypes = vFunctionEngineTypes;
	}
	
	protected void loadFunctions()
	{
		JsonUserFunctionCollection vUserFunctions = JsonLoader.loadJsonObject(new JsonLoaderOptions
													   .JsonLoaderOptionsBuilder<JsonUserFunctionCollection>()
													   .setSource(mFileDirectory, mFileName)
													   .setBeanClass(JsonUserFunctionCollection.class)
													   .shouldHookClass(JsonUserFunction.class)
													   .build()
												  );
		
		vUserFunctions.forEach(x -> mUserFunctionMap.put(x.getName(), x));
		mAreFunctionsNotLoadedYet = false;
	}
	
//	@Override
	public IUserFunction getUserFunctionByName(String pUserFunctionName)
	{
		if(mAreFunctionsNotLoadedYet) loadFunctions();
		return mUserFunctionMap.get(pUserFunctionName);
	}
	
//	@Override
	public Collection<IUserFunction> getUserFunctions()
	{
		if(mAreFunctionsNotLoadedYet) loadFunctions();
		return mUserFunctionMap.values();
	}
	
//	@Override
	public Iterable<String> getFunctionEngineTypes()
	{
		return mFunctionEngineTypes;
	}
	
	
	public static class Builder implements IBuilder<JsonUserFunctionRepository>
	{
		public static final String DEFAULT_FILE_NAME = "functions.json";
		
		private String mFileDirectory;
		private String mFileName;
		
		protected String getDefaultFileName()
		{
			return DEFAULT_FILE_NAME;
		}
		
		public Builder(String pFileDirectory)
		{
			mFileDirectory = pFileDirectory;
			mFileName = getDefaultFileName();
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
		
		@Override
		public JsonUserFunctionRepository build()
		{
			checkIfFileExists();
			return new JsonUserFunctionRepository(mFileDirectory, mFileName);
		}
		
		private void checkIfFileExists()
		{
			if(Files.notExists(Paths.get(mFileDirectory).normalize().resolve(mFileName).toAbsolutePath()))
				throw new IllegalArgumentException("The file \"" + mFileName + "\" can't be found under the folder \"" + mFileDirectory + "\".");
		}
	}
	
	public static class JsonUserFunctionCollection implements Iterable<JsonUserFunction>
	{
		@SerializedName("functions")
		@Expose
		private List<JsonUserFunction> mUserFunctions;
		
		public Iterable<JsonUserFunction> getUserFunctions()
		{
			return mUserFunctions;
		}
		
		@Override
		public Iterator<JsonUserFunction> iterator()
		{
			return mUserFunctions.iterator();
		}
	}
}
