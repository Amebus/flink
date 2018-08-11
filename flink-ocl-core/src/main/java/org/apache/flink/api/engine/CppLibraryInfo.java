package org.apache.flink.api.engine;

import java.io.Serializable;

public class CppLibraryInfo implements Serializable
{
	private String mKernelsFolder;
	
	public CppLibraryInfo(String pKernelsFolder)
	{
		mKernelsFolder = pKernelsFolder;
	}
	
	public String getKernelsFolder()
	{
		return mKernelsFolder;
	}
}
