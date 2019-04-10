package org.apache.flink.streaming;

import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.helpers.OclContextHelpers.GetIntegerTestTuples;
import static org.junit.Assert.assertEquals;

public class OclMapTest extends OclContextHelpers.OclTestClass
{
	@Override
	protected String getResourcesDirectory()
	{
		return Constants.OCL_MAP_TEST_DIR;
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
	public void OclMap_IntToString()
	{
		test(OclContextHelpers::GetIntegerTestTuples,
			 "mapIntToString",
			 (pExpected, pActual) ->
				 assertEquals(pExpected.getField(0).toString(), pActual.<String>getField(0)));
	}
	
	@Test
	public void OclMap_StringToInt()
	{
		test(() -> GetIntegerTestTuples()
			.stream()
			.map(pIOclTuple -> new Tuple1Ocl<>(pIOclTuple.<Integer>getField(0).toString()))
			.collect(Collectors.toList()),
			 "mapStringToInt",
			 ((pExpected, pActual) ->
				 assertEquals(Integer.valueOf(pExpected.getField(0)), pActual.<Integer>getField(0))));
	}
	
	protected void test(
		FITuplesGetter pOclTuplesGetter,
		String pFunctionName,
		FIAssertionAction pAction)
	{
		test(pOclTuplesGetter, (pTuples) -> getMapResult(pTuples, pFunctionName), pAction);
	}
	
	private Iterable<? extends IOclTuple> getMapResult(List<IOclTuple> pTuples, String pMapFunctionName)
	{
		return getWithCurrentContext(pOclContext ->
										 pOclContext.map(pMapFunctionName, pTuples, pTuples.size()));
	}
}
