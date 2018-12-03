package org.apache.flink.configuration;

import org.junit.Test;

import static org.apache.flink.streaming.helpers.TTypesGetter.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
	
	
	@Test
	public void TupleDefinition_WithIntegerIdentityValue_Ok()
	{
		String vIdentityValue = "10";
		Integer vValue = Integer.valueOf(vIdentityValue);
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTInteger().getT(), vIdentityValue);
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(vDefinition1);
		
		assertTrue(vDefinition1.isWithIdentityValue());
		assertTrue(vDefinition2.isWithIdentityValue());
		
		assertEquals(vValue, vDefinition1.getIdentityValue());
		assertEquals(vValue, vDefinition2.getIdentityValue());
		
		assertEquals(vValue, TupleIdentityHelper.getIntegerValue(vDefinition1));
		assertEquals(vValue, TupleIdentityHelper.getIntegerValue(vDefinition2));
		
		assertEquals(vDefinition1, vDefinition2);
		assertEquals(vDefinition2, vDefinition1);
		
		assertTrue(vDefinition1 != vDefinition2);
		
		assertEquals(vDefinition1.hashCode(), vDefinition2.hashCode());
	}
	
	@Test
	public void TupleDefinition_WithDoubleIdentityValue_Ok()
	{
		String vIdentityValue = "10.8";
		Double vValue = Double.valueOf(vIdentityValue);
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTDouble().getT(), vIdentityValue);
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(vDefinition1);
		
		assertTrue(vDefinition1.isWithIdentityValue());
		assertTrue(vDefinition2.isWithIdentityValue());
		
		assertEquals(vValue, vDefinition1.getIdentityValue());
		assertEquals(vValue, vDefinition2.getIdentityValue());
		
		assertEquals(vValue, TupleIdentityHelper.getDoubleValue(vDefinition1));
		assertEquals(vValue, TupleIdentityHelper.getDoubleValue(vDefinition2));
		
		assertEquals(vDefinition1, vDefinition2);
		assertEquals(vDefinition2, vDefinition1);
		
		assertTrue(vDefinition1 != vDefinition2);
		
		assertEquals(vDefinition1.hashCode(), vDefinition2.hashCode());
	}
	
	@Test
	public void TupleDefinition_WithStringIdentityValue_Ok()
	{
		String vIdentityValue = "ciccia";
		TupleVarDefinition vDefinition1 = new TupleVarDefinition(getJavaTString().getT(), vIdentityValue);
		TupleVarDefinition vDefinition2 = new TupleVarDefinition(vDefinition1);
		
		assertTrue(vDefinition1.isWithIdentityValue());
		assertTrue(vDefinition2.isWithIdentityValue());
		
		assertEquals(vIdentityValue, vDefinition1.getIdentityValue());
		assertEquals(vIdentityValue, vDefinition2.getIdentityValue());
		
		assertEquals(vIdentityValue, TupleIdentityHelper.getStringValue(vDefinition1));
		assertEquals(vIdentityValue, TupleIdentityHelper.getStringValue(vDefinition2));
		
		assertEquals(vDefinition1, vDefinition2);
		assertEquals(vDefinition2, vDefinition1);
		
		assertTrue(vDefinition1 != vDefinition2);
		
		assertEquals(vDefinition1.hashCode(), vDefinition2.hashCode());
	}
}
