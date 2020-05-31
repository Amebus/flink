package org.apache.flink.streaming.api.ocl.bridge;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.ocl.bridge.identity.IdentityValueToIdentityArrayConverter;
import org.apache.flink.streaming.api.ocl.common.profiling.ProfilingFile;
import org.apache.flink.streaming.api.ocl.common.profiling.ProfilingRecord;
import org.apache.flink.streaming.api.ocl.common.profiling.Stopwatch;
import org.apache.flink.streaming.api.ocl.engine.*;
import org.apache.flink.streaming.api.ocl.engine.builder.options.DefaultsValues;
import org.apache.flink.streaming.api.ocl.serialization.StreamReader;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinition;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

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
	transient private ProfilingFile mProfilingFile;
	
	// Profiling
	StopWatch mTotalStopWatch = new StopWatch();
	StopWatch mDeserStopWatch = new StopWatch();
	
	private OclBridge mOclBridgeContext;
	
	public OclContext(ISettingsRepository pSettingsRepository,
					  ITupleDefinitionRepository pTupleDefinitionRepository,
					  IUserFunctionsRepository pUserFunctionsRepository,
					  IOclContextMappings pOclContextMappings)
	{
		mProfilingFile = new ProfilingFile();
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
	
	public ISettingsRepository getSettingsRepository()
	{
		return mSettingsRepository;
	}
	
	public ITupleDefinitionRepository getTupleDefinitionRepository()
	{
		return mTupleDefinitionRepository;
	}
	
	public IUserFunctionsRepository getFunctionRepository()
	{
		return mFunctionRepository;
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
										  mOclContextMappings.getTupleBytesDimensionGetter(),
										  mOclContextMappings.getKernelBuilderMapper())
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
		// Profiling
		ProfilingRecord vProfilingRecord = new ProfilingRecord(pUserFunctionName, "filter");
		mTotalStopWatch.start();
		
		Tuple2<boolean[], Long> vFilterResult = mOclBridgeContext.filter(pUserFunctionName, pTuples, pTuplesCount);
		vProfilingRecord.setSerialization(vFilterResult.f1);
		
		mDeserStopWatch.start();
		List<IOclTuple> vResult = new LinkedList<>();
		int i = 0;
		
		for (IOclTuple vTuple : pTuples)
		{
			if(vFilterResult.f0[i])
				vResult.add(vTuple);
			i++;
		}
		mDeserStopWatch.stop();
		vProfilingRecord.setDeserialization(mDeserStopWatch.getNanoTime());
		mDeserStopWatch.reset();
		
		
		vProfilingRecord.setTotal(mTotalStopWatch.getNanoTime());
		mTotalStopWatch.reset();
		
		long[] r = mOclBridgeContext.gGetKernelProfiling(pUserFunctionName);
		vProfilingRecord.setJavaToC(r[0]);
		vProfilingRecord.setKernelComputation(r[1]);
		
		mProfilingFile.addProfilingRecord(vProfilingRecord);
		return vResult;
	}
	
	public Iterable< ? extends IOclTuple> map(
		String pUserFunctionName,
		Iterable< ? extends IOclTuple> pTuples,
		int pInputTuplesCount)
	{
		// Profiling
		ProfilingRecord vProfilingRecord = new ProfilingRecord(pUserFunctionName, "map");
		mTotalStopWatch.start();
		
		String vOutputTupleName = mFunctionRepository.getUserFunctionByName(pUserFunctionName).getOutputTupleName();
		ITupleDefinition vOutputTuple = mTupleDefinitionRepository.getTupleDefinition(vOutputTupleName);
		int vTupleDim = mOclContextMappings.getTupleBytesDimensionGetter().getTupleDimension(vOutputTuple);
		OutputTupleInfo vOutputTupleInfo = getOutputTupleInfo(vOutputTuple);
		
		Tuple2<byte[], Long> vMapResult = mOclBridgeContext.map(pUserFunctionName, pTuples, vTupleDim, vOutputTupleInfo, pInputTuplesCount);
		vProfilingRecord.setSerialization(vMapResult.f1);
		
		vProfilingRecord.setTotal(mTotalStopWatch.getNanoTime());
		mTotalStopWatch.reset();
		
		long[] r = mOclBridgeContext.gGetKernelProfiling(pUserFunctionName);
		vProfilingRecord.setJavaToC(r[0]);
		vProfilingRecord.setKernelComputation(r[1]);
		
		mProfilingFile.addProfilingRecord(vProfilingRecord);
		return mStreamReader.setStream(vMapResult.f0);
	}
	
	public <R extends IOclTuple> R reduce(
		String pUserFunctionName,
		Iterable<R> pTuples,
		int pInputTuplesCount)
	{
		// Profiling
		ProfilingRecord vProfilingRecord = new ProfilingRecord(pUserFunctionName, "reduce");
		mTotalStopWatch.start();
		
		IUserFunction vUserFunction = mFunctionRepository.getUserFunctionByName(pUserFunctionName);
		String vOutputTupleName = vUserFunction.getInputTupleName();
		ITupleDefinition vOutputTuple = mTupleDefinitionRepository.getTupleDefinition(vOutputTupleName);
		int vTupleDim = mOclContextMappings.getTupleBytesDimensionGetter().getTupleDimension(vOutputTuple);
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
		
		Tuple2<byte[], Long> vReduceResult =
			mOclBridgeContext
				.reduce(
					pUserFunctionName,
					pTuples,
					vTupleDim,
					vConverter.toIdentityArray(vIdentityValues),
					pInputTuplesCount,
					vWorkGroupSize);
		vProfilingRecord.setSerialization(vReduceResult.f1);
		
		mDeserStopWatch.start();
		R vResult = mStreamReader.setStream(vReduceResult.f0).streamReaderIterator().nextTuple();
		vProfilingRecord.setDeserialization(mDeserStopWatch.getNanoTime());
		mDeserStopWatch.reset();
		
		vProfilingRecord.setTotal(mTotalStopWatch.getNanoTime());
		mTotalStopWatch.reset();
		
		long[] r = mOclBridgeContext.gGetKernelProfiling(pUserFunctionName);
		vProfilingRecord.setJavaToC(r[0]);
		vProfilingRecord.setKernelComputation(r[1]);
		
		mProfilingFile.addProfilingRecord(vProfilingRecord);
		return vResult;
	}
	
	private OutputTupleInfo getOutputTupleInfo(ITupleDefinition pOutputTuple)
	{
		OutputTupleInfo.OutputTupleInfoBuilder vInfoBuilder =
			new OutputTupleInfo.OutputTupleInfoBuilder()
				.setArity(pOutputTuple.getArity());
		
		pOutputTuple
			.forEach(pVarDef ->
					 {
					 	byte vType = DefaultsValues.DefaultsSerializationTypes.INT;
					 	if(pVarDef.getType().startsWith("d"))
						{
							vType = DefaultsValues.DefaultsSerializationTypes.DOUBLE;
						}
					 	else if(pVarDef.getType().startsWith("s"))
						{
							vType = DefaultsValues.DefaultsSerializationTypes.STRING;
						}
						vInfoBuilder.setTType(vType);
					 });
		return vInfoBuilder.build();
	}
}
