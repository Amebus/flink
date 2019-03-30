package org.apache.flink.streaming;

import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.helpers.OclContextHelpers.*;
import static org.junit.Assert.assertEquals;

public class OclFilterTest extends OclContextHelpers.OclTestClass
{
	
	private final String FILE_NAME = "OclFilterTest.json";
	
	@Override
	protected String getFunctionsFileName()
	{
		return FILE_NAME;
	}
	
	@Override
	protected String getTuplesFileName()
	{
		return FILE_NAME;
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void OclFilterInteger()
	{
		List<IOclTuple> vTuples = GetIntegerTestTuples();
		OclContextHelpers.TupleListInfo vListInfo = new OclContextHelpers.TupleListInfo(vTuples);
		
		Iterable<? extends IOclTuple> vResult = getFilterResult(vTuples, "filterInteger");
		
		long vResultCount = StreamUtility.streamFrom(vResult).count();
		
		int vExpectedCount = vListInfo.countGreaterThan(2);
		System.out.println("OclFilterInteger - Expected: " + vExpectedCount + " - Actual: " + vResultCount);
		assertEquals(vExpectedCount, vResultCount);
		assertEquals(vListInfo.countLessOrEqualThan(2), vTuples.size() - vResultCount);
		
		List<IOclTuple> vGreaterThanTwo =
			vTuples
				.stream()
				.filter(pIOclTuple -> pIOclTuple.<Integer>getField(0) > 2)
				.collect(Collectors.toList());
		
		Iterator<IOclTuple> vIterator = vGreaterThanTwo.iterator();
		
		vResult.forEach(pO -> assertEquals(vIterator.next(), pO));
	}
	
	@Test
	public void OclFilterDouble()
	{
		
		List<IOclTuple> vTuples = GetDoubleTestTuples();
		OclContextHelpers.TupleListInfo vListInfo = new OclContextHelpers.TupleListInfo(vTuples);
		
		Iterable<? extends IOclTuple> vResult = getFilterResult(vTuples, "filterDouble");
		
		long vResultCount = StreamUtility.streamFrom(vResult).count();
		
		int vExpectedCount = vListInfo.countGreaterThan(2.0);
		System.out.println("OclFilterDouble - Expected: " + vExpectedCount + " - Actual: " + vResultCount);
		assertEquals(vExpectedCount, vResultCount);
		assertEquals(vListInfo.countLessOrEqualThan(2.0), vTuples.size() - vResultCount);
		
		List<IOclTuple> vGreaterThanTwo =
			vTuples
				.stream()
				.filter(pIOclTuple -> pIOclTuple.<Double>getField(0) > 2.0)
				.collect(Collectors.toList());
		
		Iterator<IOclTuple> vIterator = vGreaterThanTwo.iterator();
		
		vResult.forEach(pO -> assertEquals(vIterator.next(), pO));
	}
	
	private Iterable<? extends IOclTuple> getFilterResult(List<IOclTuple> pTuples, String pFilterFunctionName)
	{
		return getWithCurrentContext(pOclContext ->
										 pOclContext
											 .filter(pFilterFunctionName, pTuples, pTuples.size()));
	}
}
