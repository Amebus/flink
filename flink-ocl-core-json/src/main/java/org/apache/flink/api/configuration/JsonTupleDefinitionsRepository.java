package org.apache.flink.api.configuration;

import org.apache.flink.api.common.JsonLoader;
import org.apache.flink.api.common.JsonLoaderOptions;
import org.apache.flink.configuration.ITupleDefinitionsRepository;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonTupleDefinitionsRepository implements ITupleDefinitionsRepository<JsonTupleDefinition>
{
	private Map<String, JsonTupleDefinition> mTupleDefinitions;
	
	private boolean mTupleDefinitionsLoaded;
	private String mFileDirectory;
	private String mFileName;
	
	public JsonTupleDefinitionsRepository(String pFileDirectory)
	{
		this(pFileDirectory, "tuples.json");
	}
	
	public JsonTupleDefinitionsRepository(String pFileDirectory, String pFileName)
	{
		mTupleDefinitions = new HashMap<>();
		mFileDirectory = pFileDirectory;
		mFileName = pFileName;
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
		
		
		mTupleDefinitions = new LinkedHashMap<>();
		vMap.forEach( (k, v) -> mTupleDefinitions.put(k, new JsonTupleDefinition(k, v)));
		
		mTupleDefinitionsLoaded = true;
	}
	
	@Override
	public Iterable<JsonTupleDefinition> getTupleDefinitions()
	{
		loadTupleDefinitions();
		return mTupleDefinitions.values();
	}
	
	@Override
	public JsonTupleDefinition getTupleDefinition(String pTupleName)
	{
		loadTupleDefinitions();
		return mTupleDefinitions.get(pTupleName);
	}
}
