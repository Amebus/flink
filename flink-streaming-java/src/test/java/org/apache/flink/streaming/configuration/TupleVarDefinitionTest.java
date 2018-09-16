package org.apache.flink.streaming.configuration;

import org.apache.flink.configuration.TType;
import org.apache.flink.configuration.TupleVarDefinition;
import org.junit.Test;

import static org.apache.flink.streaming.helpers.TTypesGetter.*;
import static org.junit.Assert.*;

public class TupleVarDefinitionTest
{
	@Test
	public void TupleDefinition_GetT_Ok()
	{
		TupleVarDefinition vDefinition = new TupleVarDefinition(getJavaTString().getT());
		
		assertEquals(getJavaTString(), vDefinition.getJavaT());
		assertEquals(getCTString(), vDefinition.getCT());
		
		vDefinition = new TupleVarDefinition(TType.ConfigTypes.STRING);
		
		assertEquals(getJavaTString(), vDefinition.getJavaT());
		assertEquals(getCTString(), vDefinition.getCT());
	}
	
	@Test
	public void TupleDefinition_Hash_Ok()
	{
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTString().getT());
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(TType.ConfigTypes.STRING);
		
		assertEquals(vDefinition1.hashCode(), vDefinition2.hashCode());
	}
	
	@Test
	public void TupleDefinition_Equal_Ok()
	{
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTString().getT());
		
		assertFalse(vDefinition1.equals(null));
		assertFalse(vDefinition1.equals("ciao"));
		assertTrue(vDefinition1.equals(vDefinition1));
		
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(getCTDouble().getT());
		assertFalse(vDefinition1.equals(vDefinition2));
		assertFalse(vDefinition2.equals(vDefinition1));
	}
	
	@Test
	public void TupleDefinition_FromTupleDefinition_Ok()
	{
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTString().getT());
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(vDefinition1);
		
		assertEquals(vDefinition1, vDefinition2);
		assertEquals(vDefinition2, vDefinition1);
		
		assertTrue(vDefinition1 != vDefinition2);
		
		assertEquals(vDefinition1.hashCode(), vDefinition2.hashCode());
	}
}
