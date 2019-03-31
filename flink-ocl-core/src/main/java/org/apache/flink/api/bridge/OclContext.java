package org.apache.flink.api.bridge;

import org.apache.flink.api.bridge.identity.IdentityValueToIdentityArrayConverter;
import org.apache.flink.api.engine.*;
import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.ITupleDefinitionRepository;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class OclContext implements Serializable
{
	
	transient private ISettingsRepository mSettingsRepository;
	transient private ITupleDefinitionRepository mTupleDefinitionRepository;
	transient private IUserFunctionsRepository mFunctionRepository;
	transient private IOclContextMappings mOclContextMappings;
	
	transient private StreamReader mStreamReader;
	transient private CppLibraryInfo mCppLibraryInfo;
	
	private OclBridge mOclBridgeContext;
	
	public OclContext(ISettingsRepository pSettingsRepository,
					  ITupleDefinitionRepository pTupleDefinitionRepository,
					  IUserFunctionsRepository pUserFunctionsRepository,
					  IOclContextMappings pOclContextMappings)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleDefinitionRepository = pTupleDefinitionRepository;
		mFunctionRepository = pUserFunctionsRepository;
		mOclContextMappings = pOclContextMappings;
		
		ByteOrder vByteOrder = mSettingsRepository.getContextOptions().getNumbersByteOrdering();
		mOclBridgeContext = new OclBridge(
			mOclContextMappings
				.getNumbersByteOrderingStreamWriterMapper()
				.resolve(vByteOrder));
		mStreamReader = mOclContextMappings.getNumbersByteOrderingStreamReaderMapper().resolve(vByteOrder);
	}
	
	public void open()
	{
		generatesKernels();
		
		mOclBridgeContext.initialize(mCppLibraryInfo.getKernelsFolder());
	}
	
	public void close()
	{
		mOclBridgeContext.dispose();
		
		if(mSettingsRepository.getContextOptions().hasToRemoveTempFoldersOnClose())
			deleteLocalFiles();
	}
	
	protected void generatesKernels()
	{
		mCppLibraryInfo = new BuildEngine(mSettingsRepository,
										  mOclContextMappings.getFunctionKernelBuilderMapper(),
										  mOclContextMappings.getFunctionKernelBuilderOptionMapper())
			.generateKernels(mTupleDefinitionRepository, mFunctionRepository.getUserFunctions())
			.getCppLibraryInfo();
	}
	
	protected void deleteLocalFiles()
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
		{
			vAllFilesDeleted = vToFile.delete();
		}
		
		if(!vAllFilesDeleted)
		{
			System.out.println("Not all the kernel files has been deleted from the folder: " + vKernelsFolder.toString());
		}
	}
	
	public Iterable< ? extends IOclTuple> filter(String pUserFunctionName,
												 Iterable< ? extends IOclTuple> pTuples,
												 int pTuplesCount)
	{
		boolean[] vFilter = mOclBridgeContext.filter(pUserFunctionName, pTuples, pTuplesCount);
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
	
	public Iterable< ? extends IOclTuple> map(
		String pUserFunctionName,
		Iterable< ? extends IOclTuple> pTuples,
		int pInputTuplesCount)
	{
		
		String vOutputTupleName = mFunctionRepository.getUserFunctionByName(pUserFunctionName).getOutputTupleName();
		ITupleDefinition vOutputTuple = mTupleDefinitionRepository.getTupleDefinition(vOutputTupleName);
		int vTupleDim = mOclContextMappings.getTupleBytesDimensionGetters().getTupleDimension(vOutputTuple);
		OutputTupleInfo vOutputTupleInfo = getOutputTupleInfo(vOutputTuple);
		
		byte[] vStream = mOclBridgeContext.map(pUserFunctionName, pTuples, vTupleDim, vOutputTupleInfo, pInputTuplesCount);
		return mStreamReader.setStream(vStream);
	}
	
	public <R extends IOclTuple> R reduce(
		String pUserFunctionName,
		Iterable<R> pTuples,
		int pInputTuplesCount)
	{
		IUserFunction vUserFunction = mFunctionRepository.getUserFunctionByName(pUserFunctionName);
		String vOutputTupleName = vUserFunction.getInputTupleName();
		ITupleDefinition vOutputTuple = mTupleDefinitionRepository.getTupleDefinition(vOutputTupleName);
		int vTupleDim = mOclContextMappings.getTupleBytesDimensionGetters().getTupleDimension(vOutputTuple);
		IdentityValues vIdentityValues = new IdentityValues(vOutputTuple, vTupleDim);
		int vWorkGroupSize = vUserFunction.getWorkGroupSize();
		
		if(!vUserFunction.isWorkGroupSpecified())
		{
			throw new IllegalArgumentException("The WorkGroupSize of the Reduce function must be specified");
		}
		
		IdentityValueToIdentityArrayConverter vConverter =
			mOclContextMappings
				.getByteOrderingToIdentityValuesConverterMapper()
				.resolve(mSettingsRepository.getContextOptions().getNumbersByteOrdering());
		
		byte[] vStream =
			mOclBridgeContext
				.reduce(
					pUserFunctionName,
					pTuples,
					vTupleDim,
					vConverter.toIdentityArray(vIdentityValues),
					pInputTuplesCount,
					vWorkGroupSize);
		return mStreamReader.setStream(vStream).streamReaderIterator().nextTuple();
//		return (R) new Tuple1Ocl<>();
	}
	
	private OutputTupleInfo getOutputTupleInfo(ITupleDefinition pOutputTuple)
	{
		OutputTupleInfo.OutputTupleInfoBuilder vInfoBuilder =
			new OutputTupleInfo.OutputTupleInfoBuilder()
				.setArity(pOutputTuple.getArity());
		
		pOutputTuple
			.forEach(pVarDef ->
						 vInfoBuilder.setTType(mOclContextMappings
												   .getVarTypeToSerializationTypeMapper()
												   .resolve(pVarDef.getType())));
		return vInfoBuilder.build();
	}
}
