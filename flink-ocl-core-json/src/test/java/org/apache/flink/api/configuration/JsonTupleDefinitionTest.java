package org.apache.flink.api.configuration;

import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.TType;
import org.apache.flink.configuration.TupleVarDefinition;
import org.apache.flink.streaming.helpers.Constants;
import org.junit.Test;

import java.util.Iterator;

import static org.apache.flink.streaming.helpers.TTypesGetter.*;
import static org.junit.Assert.*;

public class JsonTupleDefinitionTest
{
	@Test
	public void TupleDefinition_Equals_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR, "tupleEqualsTest.json");
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		vDefinitions.forEach( x -> assertTrue(x.equals(x)));
		
		vDefinitions.forEach( x -> assertTrue(x.equals(new JsonTupleDefinition(x))));
		
		vDefinitions.forEach( x -> assertFalse(x.equals(null)));
		
		vDefinitions.forEach( x -> assertFalse(x.equals("string")));
		
		Iterator<JsonTupleDefinition> vIterator = vDefinitions.iterator();
		
		ITupleDefinition vOne, vTwo;
		
		vOne = vIterator.next();
		vTwo = vIterator.next();
		
		assertFalse(vOne.equals(vTwo));
		assertFalse(vTwo.equals(vOne));
		
		vTwo = vIterator.next();
		
		assertFalse(vOne.equals(vTwo));
		assertFalse(vTwo.equals(vOne));
	}
	
	@Test
	public void TupleDefinition_HashCode_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR, "tupleEqualsTest.json");
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		vDefinitions.forEach( x -> assertEquals(x.hashCode(), x.hashCode()));
		
		vDefinitions.forEach( x -> assertEquals(x.hashCode(), new JsonTupleDefinition(x).hashCode()));
		
		Iterator<JsonTupleDefinition> vIterator = vDefinitions.iterator();
		
		ITupleDefinition vOne, vTwo;
		
		vOne = vIterator.next();
		vTwo = vIterator.next();
		
		assertNotEquals(vOne.hashCode(), vTwo.hashCode());
	}
	
	@Test
	public void TupleDefinition_TWithIndexOutOfBoundIsNull_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		Iterator<JsonTupleDefinition> vIterator = vDefinitions.iterator();
		
		JsonTupleDefinition vOne = vIterator.next();
		
		assertNull(vOne.getT(5));
		assertNull(vOne.getT(JsonTupleDefinition.T_LIMIT + 1));
		
		assertNull(vOne.getJavaT(5));
		assertNull(vOne.getJavaT(JsonTupleDefinition.T_LIMIT + 1));
		
		assertNull(vOne.getCT(5));
		assertNull(vOne.getCT(JsonTupleDefinition.T_LIMIT + 1));
	}
	
	@Test
	public void TupleDefinition_Iterator_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		int expectedCount = 4;
		final int[] actualCount = {0};
		
		Iterator<JsonTupleDefinition> vTupleIterator = vDefinitions.iterator();
		
		JsonTupleDefinition vTupleDefinition = vTupleIterator.next();
		
		assertTrue(expectedCount == vTupleDefinition.getArity());
		assertEquals("tupleOne", vTupleDefinition.getName());
		
		final TType[] expectedTypes =
			{
				getJavaTInteger(),
				getJavaTString(),
				getJavaTDouble(),
				getJavaTInteger()
			};
		
		vTupleDefinition.forEach( x ->
								  {
									  assertEquals(expectedTypes[actualCount[0]], x);
									  actualCount[0]++;
								  });
		
		assertEquals(expectedCount, actualCount[0]);
		
		Iterator<TupleVarDefinition> vIterator = vTupleDefinition.tupleVarIterator();
		
		while (vIterator.hasNext())
		{
			assertTrue(vIterator.next().isWithIdentityValue());
		}
		
		expectedCount = 1;
		actualCount[0] = 0;
		expectedTypes[0] = getJavaTDouble();
		
		
		vTupleDefinition = vTupleIterator.next();
		vTupleDefinition.forEach( x ->
								  {
									  assertEquals(expectedTypes[actualCount[0]], x);
									  actualCount[0]++;
								  });
		
		assertEquals(expectedCount, actualCount[0]);
		
		
		assertFalse(vTupleIterator.hasNext());
	}
	
	@Test
	public void TupleDefinition_ReverseIterator_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		int expectedCount = 4;
		final int[] actualCount = {0};
		
		Iterator<JsonTupleDefinition> vTupleIterator = vDefinitions.iterator();
		
		JsonTupleDefinition vTupleDefinition = vTupleIterator.next();
		
		assertTrue(expectedCount == vTupleDefinition.getArity());
		assertEquals("tupleOne", vTupleDefinition.getName());
		
		final TType[] expectedTypes =
			{
				getJavaTInteger(),
				getJavaTDouble(),
				getJavaTString(),
				getJavaTInteger()
			};
		
		vTupleDefinition.reverseIterator().forEachRemaining( x ->
															 {
																 assertEquals(expectedTypes[actualCount[0]], x);
																 actualCount[0]++;
															 });
		
		assertEquals(expectedCount, actualCount[0]);
		
		expectedCount = 1;
		actualCount[0] = 0;
		expectedTypes[0] = getJavaTDouble();
		
		
		vTupleDefinition = vTupleIterator.next();
		vTupleDefinition.reverseIterator().forEachRemaining( x ->
															 {
																 assertEquals(expectedTypes[actualCount[0]], x);
																 actualCount[0]++;
															 });
		
		assertEquals(expectedCount, actualCount[0]);
		
		
		assertFalse(vTupleIterator.hasNext());
	}
	
	@Test
	public void TupleDefinition_CIterator_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		int expectedCount = 4;
		final int[] actualCount = {0};
		
		Iterator<JsonTupleDefinition> vTupleIterator = vDefinitions.iterator();
		
		JsonTupleDefinition vTupleDefinition = vTupleIterator.next();
		
		assertTrue(expectedCount == vTupleDefinition.getArity());
		assertEquals("tupleOne", vTupleDefinition.getName());
		
		final TType[] expectedTypes =
			{
				getCTInteger(),
				getCTString(),
				getCTDouble(),
				getCTInteger()
			};
		
		vTupleDefinition.cIterator().forEachRemaining( x ->
													   {
														   assertEquals(expectedTypes[actualCount[0]], x);
														   actualCount[0]++;
													   });
		
		assertEquals(expectedCount, actualCount[0]);
		
		expectedCount = 1;
		actualCount[0] = 0;
		expectedTypes[0] = getCTDouble();
		
		
		vTupleDefinition = vTupleIterator.next();
		vTupleDefinition.cIterator().forEachRemaining( x ->
													   {
														   assertEquals(expectedTypes[actualCount[0]], x);
														   actualCount[0]++;
													   });
		
		assertEquals(expectedCount, actualCount[0]);
		
		
		assertFalse(vTupleIterator.hasNext());
	}
	
	@Test
	public void TupleDefinition_ReverseCIterator_Ok()
	{
		JsonTupleDefinitionsRepository vRepository =
			new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		Iterable<JsonTupleDefinition> vDefinitions = vRepository.getTupleDefinitions();
		
		int expectedCount = 4;
		final int[] actualCount = {0};
		
		Iterator<JsonTupleDefinition> vTupleIterator = vDefinitions.iterator();
		
		JsonTupleDefinition vTupleDefinition = vTupleIterator.next();
		
		assertTrue(expectedCount == vTupleDefinition.getArity());
		assertEquals("tupleOne", vTupleDefinition.getName());
		
		final TType[] expectedTypes =
			{
				getCTInteger(),
				getCTDouble(),
				getCTString(),
				getCTInteger()
			};
		
		vTupleDefinition.cReverseIterator().forEachRemaining( x ->
															  {
																  assertEquals(expectedTypes[actualCount[0]], x);
																  actualCount[0]++;
															  });
		
		assertEquals(expectedCount, actualCount[0]);
		
		expectedCount = 1;
		actualCount[0] = 0;
		expectedTypes[0] = getCTDouble();
		
		
		vTupleDefinition = vTupleIterator.next();
		vTupleDefinition.cReverseIterator().forEachRemaining( x ->
															  {
																  assertEquals(expectedTypes[actualCount[0]], x);
																  actualCount[0]++;
															  });
		
		assertEquals(expectedCount, actualCount[0]);
		
		
		assertFalse(vTupleIterator.hasNext());
	}
}
