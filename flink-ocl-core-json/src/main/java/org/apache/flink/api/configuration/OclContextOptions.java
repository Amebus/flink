package org.apache.flink.api.configuration;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.flink.configuration.IOclContextOptions;

public class OclContextOptions implements IOclContextOptions
{
	@SerializedName("kernelsBuildFolder")
	@Expose
	private String mKernelsBuildFolder;
	
	@SerializedName("removeTempFoldersOnClose")
	@Expose
	private boolean mRemoveTempFoldersOnClose;
	
	public OclContextOptions()
	{
		mRemoveTempFoldersOnClose = true;
	}
	
	public String getKernelsBuildFolder()
	{
		return mKernelsBuildFolder;
	}
	
	public boolean hasToRemoveTempFoldersOnClose()
	{
		return mRemoveTempFoldersOnClose;
	}
}
