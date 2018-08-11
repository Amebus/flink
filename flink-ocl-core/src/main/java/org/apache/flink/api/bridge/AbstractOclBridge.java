package org.apache.flink.api.bridge;

import java.io.Serializable;

public abstract class AbstractOclBridge implements Serializable
{
	
	protected AbstractOclBridge(String pLibraryName)
	{
		System.loadLibrary(pLibraryName);
	}
	
	//Utility
	protected final native void ListDevices();
	
	//Context
	protected final native void Initialize(String pKernelsFolders);
	protected final native void Dispose();
	
	
	
	//Transformations
	protected final native byte[] OclMap(String pKernelName, byte[] pData, int[] pIndexes);
	protected final native boolean[] OclFilter(String pKernelName, byte[] pData, int[] pIndexes);
	
	//Actions
	protected final native byte[] OclReduce(String pKernelName, byte[] pData, int[] pIndexes);
}
