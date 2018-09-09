package org.apache.flink.api.engine;

import io.gsonfire.gson.HookInvocationException;
import org.apache.flink.api.common.JsonLoader;
import org.apache.flink.api.common.JsonLoaderOptions;
import org.apache.flink.streaming.api.engine.IUserFunction;
import org.apache.flink.streaming.api.engine.IUserFunctionCollection;
import org.apache.flink.streaming.api.engine.IUserFunctionsRepository;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JsonUserFunctionRepository implements IUserFunctionsRepository
{
	public static final String FUNCTIONS_FILE_NAME = "functions.json";
	
	private IUserFunctionCollection<? extends IUserFunction> mUserFunctions;
	private Map<String, IUserFunction> mUserFunctionMap;
	
	private String mFilePath;
	private String mFileName;
	
	private boolean mAreFunctionsNotLoadedYet;
	
	public JsonUserFunctionRepository(String pFilePath)
	{
		this(pFilePath, FUNCTIONS_FILE_NAME);
	}
	
	public JsonUserFunctionRepository(String pFilePath, String pFileName)
	{
		mAreFunctionsNotLoadedYet = true;
		mUserFunctionMap = new HashMap<>();
		mFilePath = pFilePath;
		mFileName = pFileName;
		checkIfFileExists();
	}
	
	@Override
	public IUserFunction getUserFunctionByName(String pUserFunctionName)
	{
		if(mAreFunctionsNotLoadedYet) loadFunctions();
		return mUserFunctionMap.get(pUserFunctionName);
	}
	
	@Override
	public Collection<IUserFunction> getUserFunctions()
	{
		if(mAreFunctionsNotLoadedYet) loadFunctions();
		return mUserFunctionMap.values();
	}
	
	private void checkIfFileExists()
	{
		if(Files.notExists(Paths.get(mFilePath).normalize().resolve(mFileName).toAbsolutePath()))
			throw new IllegalArgumentException("The file \"" + mFileName + "\" can't be found under the folder \"" + mFilePath + "\".");
	}
	
	private void loadFunctions()
	{
		try
		{
			mUserFunctions = JsonLoader.loadJsonObject(new JsonLoaderOptions
															.JsonLoaderOptionsBuilder<JsonUserFunctionCollection>()
															.setSource(mFilePath, mFileName)
															.shouldHookClass(JsonUserFunction.class)
															.setBeanClass(JsonUserFunctionCollection.class)
															.build()
													  );
			
			mUserFunctions.forEach(x -> mUserFunctionMap.put(x.getName(), x));
			mAreFunctionsNotLoadedYet = false;
		}
		catch (HookInvocationException ex)
		{
			throw new IllegalArgumentException(ex.getCause());
		}
	}
}
