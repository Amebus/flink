package org.apache.flink.streaming.api.ocl.engine;

import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.KernelBuilderMapper;
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
	private ITupleBytesDimensionGetter mTupleBytesDimensionGetter;
	private KernelBuilderMapper mKernelBuilders;
	
	public KernelCodeBuilderEngine(
		ISettingsRepository pSettingsRepository,
		ITupleDefinitionRepository pTupleDefinitionRepository,
		Iterable<? extends IUserFunction> pUserFunctions,
		ITupleBytesDimensionGetter pTupleBytesDimensionGetter,
		KernelBuilderMapper pKernelBuilderMapper)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleDefinitionRepository = pTupleDefinitionRepository;
		mUserFunctions = pUserFunctions;
		mTupleBytesDimensionGetter = pTupleBytesDimensionGetter;
		mKernelBuilders = pKernelBuilderMapper;
	}
	
	public ISettingsRepository getSettingsRepository()
	{
		return mSettingsRepository;
	}
	public ITupleDefinitionRepository getTupleDefinitionRepository()
	{
		return mTupleDefinitionRepository;
	}
	public Iterable<? extends IUserFunction> getUserFunctions()
	{
		return mUserFunctions;
	}
	public ITupleBytesDimensionGetter getTupleBytesDimensionGetter()
	{
		return mTupleBytesDimensionGetter;
	}
	public KernelBuilderMapper getKernelBuilders()
	{
		return mKernelBuilders;
	}
	
	public CppLibraryInfo generateKernels()
	{
		List<OclKernel> vResult = new LinkedList<>();
		for (IUserFunction vUserFunction : getUserFunctions())
		{
			vResult.add(generateKernel(vUserFunction));
		}
		
		String vKernelsFolderPrefix = getSettingsRepository().getContextOptions().getKernelsBuildFolder();
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
		return getKernelBuilders()
			.resolve(pUserFunction.getType())
			.setPDAKernelBuilderOptions(
				new KernelBuilderOptions(
					pUserFunction,
					getTupleDefinitionRepository(),
					getTupleBytesDimensionGetter(),
					getSettingsRepository().getContextOptions(),
					getSettingsRepository().getKernelsOptions()))
			.build();
	}
	
	private void saveKernelsFiles(Path pKernelsFolder, List<OclKernel> pKernels)
	{
		pKernels.forEach( k ->
						  {
							  Path vFile = pKernelsFolder.resolve(k.getName() + ".knl");
							  List<String> lines = new ArrayList<>(2);
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
