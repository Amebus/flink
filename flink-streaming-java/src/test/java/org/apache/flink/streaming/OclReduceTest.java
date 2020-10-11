package org.apache.flink.streaming;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.streaming.api.ocl.bridge.OclContext;
import org.apache.flink.streaming.api.ocl.engine.BuildEngine;
import org.apache.flink.streaming.api.ocl.engine.builder.FilterKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.MapKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.ReduceKernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.KernelBuilderMapper;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;
import org.apache.flink.streaming.api.ocl.tuple.Tuple1Ocl;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.helpers.OclContextHelpers.GetIntegerZeroMeanTuples;
import static org.junit.Assert.assertEquals;

public class OclReduceTest extends OclContextHelpers.OclTestClass
{
	@Override
	protected String getResourcesDirectory()
	{
		return Constants.OCL_REDUCE_TEST_DIR;
	}
	
	@Override
	protected String getOclSettingsDirectory()
	{
		return getResourcesDirectory();
	}
	
	@Override
	protected String getFunctionsDirectory()
	{
		return getResourcesDirectory();
	}
	
	@Override
	protected String getTuplesDirectory()
	{
		return getResourcesDirectory();
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	
	private final String TEMPLATE_STRING_PATTERN = "<\\[.+]>";
	private final Pattern TEMPLATE_PATTERN = Pattern.compile(TEMPLATE_STRING_PATTERN);
//	@Test
	public void AAAAAAAAAAAAAAAA()
	{
		String vTemplate = "\n" +
						   "<[helper-functions]>\n" +
						   "\n" +
						   "<[defines]>\n" +
						   "\n" +
						   "__kernel void <[kernel-name]>(\n" +
						   "    <[kernel-args]>)\n" +
						   "{\n" +
						   "    <[kernel-code]>\n" +
						   "}";
		
		Matcher vMatcher = TEMPLATE_PATTERN.matcher(vTemplate);
		
		boolean vTemplateFound = vMatcher.find();
		
		OclContext vHelper = getNewOclContext();
		
		
		KernelBuilderMapper vMapper = new KernelBuilderMapper();
		vMapper.register("map", new MapKernelBuilder());
		vMapper.register("filter", new FilterKernelBuilder());
		vMapper.register("reduce", new ReduceKernelBuilder());
//		BuildEngine vBuildEngine = new BuildEngine(
//			vHelper.getSettingsRepository(),
//			vMapper);
//
//		vBuildEngine.generateKernels(vHelper.getTupleDefinitionRepository(), vHelper.getFunctionRepository().getUserFunctions());
		int i = 10 + 7;
	}
	
	@Test
	public void OclReduceInteger()
	{
		List<IOclTuple> vTuples = GetIntegerZeroMeanTuples(3);
		
		for(IOclTuple vTuple : vTuples)
		{
			System.out.print(" ");
			System.out.print(vTuple.getFieldOcl(0));
			System.out.print(" ");
		}
		
		System.out.println();
		
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(5));
		vTuples.add(new Tuple1Ocl<>(2));
		vTuples.add(new Tuple1Ocl<>(6));
//		vTuples.add(new Tuple1Ocl<>(-3));
//
		vTuples.add(new Tuple1Ocl<>(3));
		vTuples.add(new Tuple1Ocl<>(2));
		vTuples.add(new Tuple1Ocl<>(4));
//		vTuples.add(new Tuple1Ocl<>(1));

//		vTuples = GetIntegerZeroMeanTuples();
		
		int expected = vTuples.stream().mapToInt(pT -> pT.getField(0)).sum();
		System.out.println("Expected: " + expected);
		
		
		IOclTuple vResult = getReduceResult(vTuples, "reduceInteger");
		assertEquals(
			expected,
			(int)vResult.getField(0));
	}
	
	private IOclTuple getReduceResult(List<IOclTuple> pTuples, String pReduceFunctionName)
	{
		return getWithCurrentContext(pOclContext -> pOclContext.reduce(pReduceFunctionName, pTuples, pTuples.size()));
	}
}
