package org.apache.flink.api.engine.kernel;

import org.apache.flink.api.common.OnDemandLoader;
import org.apache.flink.api.common.mappers.StringKeyMapper;
import org.apache.flink.api.engine.CppLibraryInfo;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.kernel.OclKernel;
import org.apache.flink.api.engine.kernel.builder.*;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;

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
	private ITupleDefinitionsRepository mTupleDefinitionsRepository;
	private Iterable<? extends IUserFunction> mUserFunctions;
	private KernelBuilderOptions.KernelOptionsBuilder mKernelBuilderOptionsBuilder;
	private StringKeyMapper<KernelBuilder, KernelBuilderOptions> mTypeKernelBuilderMapper;
	
	
	public KernelCodeBuilderEngine(ISettingsRepository pSettingsRepository, ITupleDefinitionsRepository pTupleDefinitionsRepository, Iterable<? extends IUserFunction> pUserFunctions)
	{
		mSettingsRepository = pSettingsRepository;
		mTupleDefinitionsRepository = pTupleDefinitionsRepository;
		mUserFunctions = pUserFunctions;
		
		mKernelBuilderOptionsBuilder = new KernelBuilderOptions.KernelOptionsBuilder()
			.setTupleDefinitionsRepository(mTupleDefinitionsRepository)
			.setContextOptions(mSettingsRepository.getContextOptions())
			.setKernelOptions(mSettingsRepository.getKernelsOptions());
		
		setUpMapper();
	}
	
	private void setUpMapper()
	{
		mTypeKernelBuilderMapper = new StringKeyMapper<>();
		
		//Transformations
		mTypeKernelBuilderMapper.register(IUserFunction.MAP, new OnDemandLoader<>(MapBuilder::new));
		//mTypeKernelBuilderMapper.register(IUserFunction.FLAT_MAP, new OnDemandLoader<>(FlatMapBuilder::new));
		mTypeKernelBuilderMapper.register(IUserFunction.FILTER, new OnDemandLoader<>(FilterBuilder::new));
		
		//Actions
		mTypeKernelBuilderMapper.register(IUserFunction.REDUCE, new OnDemandLoader<>(ReduceBuilder::new));
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
	
	private void saveKernelsFiles(Path pKernelsFolder, List<OclKernel> pKernels)
	{
		pKernels.forEach( k ->
						  {
							  Path vFile = pKernelsFolder.resolve(k.getName() + ".knl");
							  List<String> lines = new ArrayList<>(1);
							  lines.add(k.getCode());
							  System.out.println(vFile.toAbsolutePath().toString());
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
	
	private OclKernel generateKernel(IUserFunction pUserFunction)
	{
		return mTypeKernelBuilderMapper.resolve(pUserFunction.getType(),
												mKernelBuilderOptionsBuilder
													.setUserFunction(pUserFunction)
													.build()
											   ).build();
	}
}
