package org.apache.flink.streaming.api.ocl.common;

import io.gsonfire.GsonFireBuilder;
import io.gsonfire.gson.HookInvocationException;

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
		
		T vResult;
		
		try
		{
			vResult = vBuilder.createGson().fromJson(vReader, pOptions.getBeanClass());
		}
		catch (HookInvocationException ex)
		{
			throw new IllegalArgumentException(ex.getCause());
		}
		
		return vResult;
	}
}
