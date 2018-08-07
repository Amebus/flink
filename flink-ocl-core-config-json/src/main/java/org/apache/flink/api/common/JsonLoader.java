package org.apache.flink.api.common;

import io.gsonfire.GsonFireBuilder;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class JsonLoader
{
	public static <T> T loadJsonObject(JsonLoaderOptions<T> pOptions)
	{
		FileReader vReader;
		try
		{
			vReader = new FileReader(pOptions.getFile());
		}
		catch (FileNotFoundException pE)
		{
			throw new IllegalArgumentException("The file: " + pOptions.getFile().getAbsolutePath() + " was not found", pE);
		}
		
		GsonFireBuilder vBuilder = new GsonFireBuilder();
		
		pOptions.getClassesToHook().forEach(vBuilder::enableHooks);
		
		return vBuilder.createGson().fromJson(vReader, pOptions.getBeanClass());
	}
}
