package org.apache.flink.api.engine;

public class CppLibraryInfo
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
