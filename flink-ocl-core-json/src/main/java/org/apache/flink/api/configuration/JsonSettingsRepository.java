package org.apache.flink.api.configuration;

import org.apache.flink.api.common.JsonLoader;
import org.apache.flink.api.common.JsonLoaderOptions;
import org.apache.flink.streaming.configuration.IOclContextOptions;
import org.apache.flink.streaming.configuration.IOclKernelsOptions;
import org.apache.flink.streaming.configuration.IOclSettings;
import org.apache.flink.streaming.configuration.ISettingsRepository;

public class JsonSettingsRepository implements ISettingsRepository
{
	private IOclSettings mOclSettings;
	private boolean mSettingsLoaded;
	private String mFileDirectory;
	private String mFileName;
	
	
	public JsonSettingsRepository(String pFileDirectory)
	{
		this(pFileDirectory, "oclSettings.json");
	}
	
	public JsonSettingsRepository(String pFileDirectory, String pFileName)
	{
		mSettingsLoaded = false;
		mFileDirectory = pFileDirectory;
		mFileName = pFileName;
	}
	
	private void loadSettings()
	{
		if(mSettingsLoaded)
			return;
		mOclSettings = JsonLoader.loadJsonObject(new JsonLoaderOptions.JsonLoaderOptionsBuilder<OclSettings>()
													 .setSource(mFileDirectory, mFileName)
													 .setBeanClass(OclSettings.class)
													 .build());
		mSettingsLoaded = true;
	}
	
	@Override
	public IOclContextOptions getContextOptions()
	{
		loadSettings();
		return mOclSettings.getContextOptions();
	}
	
	@Override
	public IOclKernelsOptions getKernelsOptions()
	{
		loadSettings();
		return mOclSettings.getOclKernelOptions();
	}
}
