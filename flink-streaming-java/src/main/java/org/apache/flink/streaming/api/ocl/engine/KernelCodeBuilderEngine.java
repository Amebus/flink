package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.FunctionKernelBuilderOptionMapper;
import org.apache.flink.streaming.configuration.ISettingsRepository;
import org.apache.flink.streaming.configuration.ITupleDefinitionRepository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class KernelCodeBuilderEngine
{
	private ISettingsRepository mSettingsRepository;
	private ITupleDefinitionRepository mTupleDefinitionRepository;
	private Iterable<? extends IUserFunction> mUserFunctions;
	private FunctionKernelBuilderMapper mTypeKernelBuilderMapper;
	private FunctionKernelBuilderOptionMapper mKernelBuilderOptionMapping;
	
	public KernelCodeBuilderEngine(
		ISettingsRepository pSettingsRepository,
		ITupleDefinitionRepository pTupleDefinitionRepository,
		Iterable<? extends IUserFunction> pUserFunctions,
		FunctionKernelBuilderMapper pTypeKernelBuilderMapper,
		FunctionKernelBuilderOptionMapper pKernelBuilderOptionMapping)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleDefinitionRepository = pTupleDefinitionRepository;
		mUserFunctions = pUserFunctions;
		mTypeKernelBuilderMapper = pTypeKernelBuilderMapper;
		mKernelBuilderOptionMapping = pKernelBuilderOptionMapping;
		
		setUpMappings();
	}
	
	protected void setUpMappings()
	{
		mKernelBuilderOptionMapping
			.getKeys()
			.forEach(pKey ->
						 mKernelBuilderOptionMapping
							 .resolve(pKey)
							 .setTupleDefinitionRepository(mTupleDefinitionRepository)
							 .setContextOptions(mSettingsRepository.getContextOptions())
							 .setKernelOptions(mSettingsRepository.getKernelsOptions()));
	}
	
	public CppLibraryInfo generateKernels()
	{
		List<OclKernel> vResult = new LinkedList<>();
		for (IUserFunction vUserFunction : mUserFunctions)
		{
			vResult.add(generateKernel(vUserFunction));
		}
		
		String vKernelsFolderPrefix = mSettingsRepository.getContextOptions().getKernelsBuildFolder();
		Path vKernelsFolder;
		try
		{
			vKernelsFolder = Files.createTempDirectory(vKernelsFolderPrefix);
		}
		catch (IOException pE)
		{
			throw new IllegalArgumentException("It was not possible to create a temporary folder with the specified" +
											   "prefix: " + vKernelsFolderPrefix, pE);
		}
		
		saveKernelsFiles(vKernelsFolder, vResult);
		
		return new CppLibraryInfo(vKernelsFolder.toAbsolutePath().toString());
	}
	
	private OclKernel generateKernel(IUserFunction pUserFunction)
	{
		return mTypeKernelBuilderMapper.resolve(pUserFunction.getType(),
												mKernelBuilderOptionMapping
													 .resolve(pUserFunction.getType())
														.setUserFunction(pUserFunction)
														.build()
											   ).build();
	}
	
	private void saveKernelsFiles(Path pKernelsFolder, List<OclKernel> pKernels)
	{
		pKernels.forEach( k ->
						  {
							  Path vFile = pKernelsFolder.resolve(k.getName() + ".knl");
							  List<String> lines = new ArrayList<>(1);
							  lines.add(k.getCode());
//							  System.out.println(vFile.toAbsolutePath().toString());
							  try
							  {
								  if(Files.exists(vFile))
								  {
									  Files.delete(vFile);
								  }
				
								  Files.write(vFile, lines, StandardCharsets.UTF_8);
							  }
							  catch (IOException pE)
							  {
								  throw new IllegalArgumentException("It was not possible to create the kernel file: " +
																	 vFile.toAbsolutePath(), pE);
							  }
						  });
	}
}
