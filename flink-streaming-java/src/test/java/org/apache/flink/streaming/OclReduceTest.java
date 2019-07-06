package org.apache.flink.streaming;

import org.apache.flink.streaming.api.ocl.engine.OclKernel;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelTemplatesRepository;
import org.apache.flink.streaming.api.ocl.engine.builder.PDAKernelBuilder;
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
	@Test
	public void AAAAAAAAAAAAAAAA()
	{
		String vTemplate = "\n" +
						   "<[helper-functions]>\n" +
						   "\n" +
						   "<[defines]>\n" +
						   "\n" +
						   "__kernel void <[kernel-name/]>(\n" +
						   "    <[kernel-args]>)\n" +
						   "{\n" +
						   "    <[kernel-code]>\n" +
						   "}";
		
		Matcher vMatcher = TEMPLATE_PATTERN.matcher(vTemplate);
		
		boolean vTemplateFound = vMatcher.find();
		
		IKernelTemplatesRepository vRepository = new IKernelTemplatesRepository()
		{
			@Override
			public String getRootTemplateCode()
			{
				return vTemplate;
			}
			
			@Override
			public String getTemplateCode(String pTemplateName)
			{
				String vResult;
				switch (pTemplateName)
				{
					case "<[helper-functions]>":
						vResult = "--repo hf--";
						break;
					case "<[defines]>":
						vResult = "--repo defines--";
						break;
					case "<[kernel-name/]>":
						vResult = "--repo kernel name--";
						break;
					case "<[kernel-args]>":
						vResult = "--repo kernel args--";
						break;
					case "<[kernel-code]>":
						vResult = "<[kernel-code-a]><[kernel-code-b]>";
						break;
					case "<[kernel-code-a]>":
						vResult = "--repo kernel code a--";
						break;
					case "<[kernel-code-b]>":
						vResult = "--repo kernel code b--";
						break;
					default:
						vResult = "--";
				}
				return vResult;
			}
		};
//		PDAKernelBuilder vPDAKernelBuilder = new PDAKernelBuilder(vTemplate)
//		{
//		};
//
//
//		OclKernel vRe = vPDAKernelBuilder.build();
		
//		System.out.println(vRe.getCode());
		
		int i = 10 + 7;
	}
	
	//@Test
	public void OclReduceInteger()
	{
		List<IOclTuple> vTuples = new ArrayList<>(2);
		
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(5));
		vTuples.add(new Tuple1Ocl<>(73));
		vTuples.add(new Tuple1Ocl<>(10));

//		vTuples = GetIntegerZeroMeanTuples();
		
		IOclTuple vResult = getReduceResult(vTuples, "reduceInteger");
		
		assertEquals(
			vTuples.stream().mapToInt(pT -> pT.getField(0)).sum(),
			(int)vResult.getField(0));
	}
	
	private IOclTuple getReduceResult(List<IOclTuple> pTuples, String pReduceFunctionName)
	{
		return getWithCurrentContext(pOclContext -> pOclContext.reduce(pReduceFunctionName, pTuples, pTuples.size()));
	}
}
