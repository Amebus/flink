package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.FIGenerateExtra;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilderPlugin;

public interface IPluginWithExtra extends IKernelBuilderPlugin
{
	<T> T getExtra(String pKey);
	default <T> T getExtra(String pKey, FIGenerateExtra<T> pGenerateExtra)
	{
		if(pGenerateExtra == null)
			throw new IllegalArgumentException(("can't be null"));
		
		T vResult = getExtra(pKey);
		if(vResult == null)
		{
			vResult = pGenerateExtra.generateExtra();
		}
		setExtra(pKey, vResult);
		return vResult;
	}
	
	IPluginWithExtra setExtra(String pKey, Object pExtra);
	<T> T removeExtra(String pKey);
}
