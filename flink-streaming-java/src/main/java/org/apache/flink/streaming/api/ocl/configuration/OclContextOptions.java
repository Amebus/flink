package org.apache.flink.streaming.api.ocl.configuration;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import io.gsonfire.annotations.PostDeserialize;
import org.apache.flink.streaming.configuration.IOclContextOptions;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class OclContextOptions implements IOclContextOptions
{
	@SerializedName("removeTempFoldersOnClose")
	@Expose
	private boolean mRemoveTempFoldersOnClose;
	
	@SerializedName("numbersByteOrdering")
	@Expose
	private String mJsonNumbersByteOrdering;
	
	private ByteOrder mNumbersByteOrdering;
	
	@SerializedName("kernelsBuildFolder")
	@Expose
	private String mKernelsBuildFolder;
	
	@SerializedName("kernelSourcePaths")
	@Expose
	private Map<String, String> mKernelSourcePaths;
	
	public OclContextOptions()
	{
		mRemoveTempFoldersOnClose = true;
		mNumbersByteOrdering = ByteOrder.LITTLE_ENDIAN;
		mKernelSourcePaths = new HashMap<>();
	}
	
	@Override
	public boolean hasToRemoveTempFoldersOnClose()
	{
		return mRemoveTempFoldersOnClose;
	}
	
	@Override
	public ByteOrder getNumbersByteOrdering()
	{
		return mNumbersByteOrdering;
	}
	
	@Override
	public String getKernelsBuildFolder()
	{
		return mKernelsBuildFolder;
	}
	
	@Override
	public String getKernelSourcePath(String pKernelType)
	{
		String vResult = mKernelSourcePaths.get(pKernelType);
		if (vResult == null)
			vResult = "../Cpp/Code/Sources/$.template".replace("$", pKernelType);
		return vResult;
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
