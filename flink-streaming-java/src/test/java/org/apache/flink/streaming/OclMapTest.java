package org.apache.flink.streaming;

import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
	public void OclMapSimple_IntToString()
	{
		List<IOclTuple> vTuples =
			GetIntegerTestTuples();
//			new ArrayList<>(2);
//		vTuples.add(new Tuple1Ocl<>(-1679099059));
//		vTuples.add(new Tuple1Ocl<>(528136394));
		
		Iterable<? extends IOclTuple> vResult = getMapResult(vTuples, "mapIntToString");
		
		Iterator<IOclTuple> vIterator = vTuples.iterator();
		vResult.forEach(pOclTuple ->
						{
							IOclTuple vTuple = vIterator.next();
							assertEquals(vTuple.getField(0).toString(), pOclTuple.<String>getField(0));
						});
	}
	
	@Test
	public void OclMapSimple_StringToInt()
	{
		List<IOclTuple> vTuples = GetIntegerTestTuples()
			.stream()
			.map(pIOclTuple -> new Tuple1Ocl<>(pIOclTuple.<Integer>getField(0).toString()))
			.collect(Collectors.toList());
		
		Iterable<? extends IOclTuple> vResult = getMapResult(vTuples, "mapStringToInt");
		
		Iterator<IOclTuple> vIterator = vTuples.iterator();
		vResult.forEach(pOclTuple ->
						{
							IOclTuple vTuple = vIterator.next();
							assertEquals(Integer.valueOf(vTuple.getField(0)), pOclTuple.<Integer>getField(0));
						});
		
	}
	
	private Iterable<? extends IOclTuple> getMapResult(List<IOclTuple> pTuples, String pMapFunctionName)
	{
		return getWithCurrentContext(pOclContext ->
									pOclContext.map(pMapFunctionName, pTuples, pTuples.size()));
	}
}
