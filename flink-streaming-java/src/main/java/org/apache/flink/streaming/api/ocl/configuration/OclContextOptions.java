package org.apache.flink.streaming.api.ocl.configuration;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import io.gsonfire.annotations.PostDeserialize;
import org.apache.flink.streaming.configuration.IOclContextOptions;

import java.nio.ByteOrder;

public class OclContextOptions implements IOclContextOptions
{
	@SerializedName("kernelsBuildFolder")
	@Expose
	private String mKernelsBuildFolder;
	
	@SerializedName("removeTempFoldersOnClose")
	@Expose
	private boolean mRemoveTempFoldersOnClose;
	
	@SerializedName("numbersByteOrdering")
	@Expose
	private String mJsonNumbersByteOrdering;
	
	private ByteOrder mNumbersByteOrdering;
	
	public OclContextOptions()
	{
		mRemoveTempFoldersOnClose = true;
		mNumbersByteOrdering = ByteOrder.LITTLE_ENDIAN;
	}
	
	public String getKernelsBuildFolder()
	{
		return mKernelsBuildFolder;
	}
	
	public boolean hasToRemoveTempFoldersOnClose()
	{
		return mRemoveTempFoldersOnClose;
	}
	
	@Override
	public ByteOrder getNumbersByteOrdering()
	{
		return mNumbersByteOrdering;
	}
	
	@PostDeserialize
	private void postDeserialize()
	{
		if(mJsonNumbersByteOrdering != null)
		{
			String vUpper = mJsonNumbersByteOrdering.toUpperCase();
			if(vUpper.contains("BIG"))
			{
				mNumbersByteOrdering = ByteOrder.BIG_ENDIAN;
			}
		}
	}
}
