package org.apache.flink.streaming.api.bridge;

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
	
	/**
	 *
	 * @param pKernelName
	 * @param pData
	 * @param pIndexes
	 * @param pOutputTupleDimension
	 * @param pOutputTupleSignature The signature of the output tuple i.e. the arity plus the type of the elements
	 * @return The resulting stream of bytes of the OpenCL computation
	 * (to be used by {@link org.apache.flink.api.serialization.StreamReader})
	 */
	protected final native byte[] OclMap(String pKernelName, byte[] pData, int[] pIndexes,
										 int pOutputTupleDimension, byte[] pOutputTupleSignature);
	protected final native boolean[] OclFilter(String pKernelName, byte[] pData, int[] pIndexes);
	
	//Actions
	
	/**
	 *
	 * @param pKernelName
	 * @param pData
	 * @param pIndexes
	 * @param pOutputTupleDimension
	 * @param pOutputTupleArity The signature of the output tuple i.e. the arity plus the type of the elements
	 * @return The resulting stream of bytes of the OpenCL computation
	 * (to be used by {@link org.apache.flink.api.serialization.StreamReader})
	 */
	protected final native byte[] OclReduce(String pKernelName, byte[] pData, int[] pIndexes,
											int pOutputTupleDimension, byte[] pOutputTupleArity);
}
