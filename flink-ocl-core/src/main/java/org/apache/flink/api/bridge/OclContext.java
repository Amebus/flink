package org.apache.flink.api.bridge;

import org.apache.flink.api.engine.BuildEngine;
import org.apache.flink.api.engine.CppLibraryInfo;
import org.apache.flink.api.engine.IUserFunctionsRepository;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;
import org.apache.flink.api.bridge.OclBridge;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class OclContext implements Serializable
{
	transient private ISettingsRepository mSettingsRepository;
	transient private ITupleDefinitionsRepository mTupleDefinitionsRepository;
	transient private IUserFunctionsRepository mFunctionRepository;
	
	transient private CppLibraryInfo mCppLibraryInfo;
	
	private OclBridge mOclBridgeContext;
	
	public OclContext(ISettingsRepository pSettingsRepository,
					  ITupleDefinitionsRepository pTupleDefinitionsRepository,
					  IUserFunctionsRepository pUserFunctionsRepository)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mFunctionRepository = pUserFunctionsRepository;
		mOclBridgeContext = new OclBridge();
	}
	
	public void open()
	{
		generatesKernels();
		
//		mOclBridgeContext.initialize(mCppLibraryInfo.getKernelsFolder());
	}
	
	public void close()
	{
		mOclBridgeContext.dispose();
		
		if(mSettingsRepository.getContextOptions().hasToRemoveTempFoldersOnClose())
			deleteLocalFiles();
	}
	
	private void generatesKernels()
	{
		mCppLibraryInfo = new BuildEngine(mSettingsRepository)
			.generateKernels(mTupleDefinitionsRepository, mFunctionRepository.getUserFunctions())
			.getCppLibraryInfo();
	}
	
	private void deleteLocalFiles()
	{
		Path vKernelsFolder = Paths.get(mCppLibraryInfo.getKernelsFolder());
		
		File vToFile = vKernelsFolder.toFile();
		
		if (vToFile.isFile())
			return;
		
		boolean vAllFilesDeleted = true;
		for (File vFile : Objects.requireNonNull(vToFile.listFiles()))
		{
			vAllFilesDeleted &= vFile.delete();
		}
		
		if (vAllFilesDeleted)
			vToFile.delete();
	}
	
	public Iterable< ? extends IOclTuple> filter(String pUserFunctionName, List< ? extends IOclTuple> pTuples)
	{
		boolean[] vFilter = mOclBridgeContext.filter(pUserFunctionName, pTuples);
		List<IOclTuple> vResult = new LinkedList<>();
		int i = 0;
		
		for (IOclTuple vTuple : pTuples)
		{
			if(vFilter[i])
				vResult.add(vTuple);
			i++;
		}
		return vResult;
	}
	
	public Iterable< ? extends IOclTuple> map(String pUserFunctionName, List< ? extends IOclTuple> pTuples)
	{
		byte[] vStream = mOclBridgeContext.map(pUserFunctionName, pTuples);
		return StreamReader.getStreamReader().setStream(vStream);
	}
	
}
