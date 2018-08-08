package org.apache.flink.api.configuration;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.flink.configuration.IOclContextOptions;
import org.apache.flink.configuration.IOclKernelsOptions;
import org.apache.flink.configuration.IOclSettings;

public class OclSettings implements IOclSettings
{
	@SerializedName("contextOptions")
	@Expose
	private OclContextOptions mContextOptions;
	
	@SerializedName("kernelOptions")
	@Expose
	private OclKernelsOptions mOclKernelOptions;
	
	public OclSettings()
	{
		mContextOptions = new OclContextOptions();
		mOclKernelOptions = new OclKernelsOptions();
	}
	
	@Override
	public IOclContextOptions getContextOptions()
	{
		return mContextOptions;
	}
	
	@Override
	public IOclKernelsOptions getOclKernelOptions()
	{
		return mOclKernelOptions;
	}
}
