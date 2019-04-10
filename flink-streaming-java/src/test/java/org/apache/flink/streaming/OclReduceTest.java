package org.apache.flink.streaming;

import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

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
	
	
	@Test
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
