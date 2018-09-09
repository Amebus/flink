package org.apache.flink.configuration;

import org.junit.Test;

import static org.apache.flink.streaming.helpers.TTypesGetter.*;
import static org.junit.Assert.*;

public class TTypeTest
{
	@Test
	public void JavaTypes_Builder_Ok()
	{
		TType vType;
		
		//String
		vType = getJavaTString();
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		vType = getJavaTCString();
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		//String:100
		vType = getJavaTString(100);
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		vType = getJavaTCString(100);
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		
		//Integer
		vType = getJavaTInteger();
		assertEquals(vType.getT(), JavaTType.JavaTypes.INTEGER);
		vType = getJavaTInt();
		assertEquals(vType.getT(), JavaTType.JavaTypes.INTEGER);
		
		//Integer
		vType = getJavaTDouble();
		assertEquals(vType.getT(), JavaTType.JavaTypes.DOUBLE);
	}
	
	@Test
	public void CTypes_Builder_Ok()
	{
		TType vType;
		
		//String
		vType = getCTString();
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		vType = getCTCString();
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		//String:100
		vType = getCTString(100);
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		vType = getCTCString(100);
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		
		//Integer
		vType = getCTInteger();
		assertEquals(vType.getT(), CTType.CTypes.INTEGER);
		vType = getCTInt();
		assertEquals(vType.getT(), CTType.CTypes.INTEGER);
		
		//Integer
		vType = getCTDouble();
		assertEquals(vType.getT(), CTType.CTypes.DOUBLE);
	}
	
	@Test
	public void TTypes_HashCode_Ok()
	{
		TType vType1, vType2;
		
		vType1 = getJavaTString();
		vType2 = getJavaTString();
		
		assertTrue(vType1.equals(vType2));
		assertEquals(vType1.hashCode(), vType2.hashCode());
		
		vType2 = getJavaTInteger();
		assertFalse(vType1.equals(vType2));
		assertNotEquals(vType1.hashCode(), vType2.hashCode());
		
		vType1 = getJavaTInteger();
		assertTrue(vType1.equals(vType2));
		assertEquals(vType1.hashCode(), vType2.hashCode());
		
		vType2 = getJavaTDouble();
		assertFalse(vType1.equals(vType2));
		assertNotEquals(vType1.hashCode(), vType2.hashCode());
		
		vType1 = getJavaTDouble();
		assertTrue(vType1.equals(vType2));
		assertEquals(vType1.hashCode(), vType2.hashCode());
		
	}
	
	@Test
	public void TTypes_Builder_Error()
	{
		javaTTypeError(null);
		javaTTypeError("");
		javaTTypeError("a");
		
		cTTypeError(null);
		cTTypeError("");
		cTTypeError("a");
	}
	
	private void javaTTypeError(String prmType)
	{
		boolean vExceptionRaised = false;
		
		try
		{
			new JavaTType.Builder(prmType).build();
		}
		catch (IllegalArgumentException ex)
		{
			vExceptionRaised = true;
		}
		
		assertTrue(vExceptionRaised);
	}
	
	private void cTTypeError(String prmType)
	{
		boolean vExceptionRaised = false;
		
		try
		{
			new CTType.Builder(prmType).build();
		}
		catch (IllegalArgumentException ex)
		{
			vExceptionRaised = true;
		}
		
		assertTrue(vExceptionRaised);
	}
	
	@Test
	public void TTypes_Builder_String_Ok()
	{
		TType vType;
		
		//String
		vType = getJavaTString();
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(100, vType.getMaxByteOccupation());
		
		vType = getJavaTCString();
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(100, vType.getMaxByteOccupation());
		
		//String
		vType = getCTString();
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(100, vType.getMaxByteOccupation());
		
		vType = getCTCString();
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(100, vType.getMaxByteOccupation());
	}
	
	@Test
	public void TTypes_Builder_StringWithMaxDim_Ok()
	{
		TType vType;
		
		//String
		vType = getJavaTString(33);
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(33, vType.getMaxByteOccupation());
		
		vType = getJavaTCString(33);
		assertEquals(vType.getT(), JavaTType.JavaTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(33, vType.getMaxByteOccupation());
		
		//String
		vType = getCTString(33);
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(33, vType.getMaxByteOccupation());
		
		vType = getCTCString(33);
		assertEquals(vType.getT(), CTType.CTypes.STRING);
		assertEquals(-1, vType.getByteOccupation());
		assertEquals(33, vType.getMaxByteOccupation());
	}
	
	@Test
	public void TTypes_Builder_Integer_Ok()
	{
		TType vType;
		
		//
		vType = getJavaTInteger();
		assertEquals(vType.getT(), JavaTType.JavaTypes.INTEGER);
		assertEquals(4, vType.getByteOccupation());
		assertEquals(4, vType.getMaxByteOccupation());
		
		vType = getJavaTInt();
		assertEquals(vType.getT(), JavaTType.JavaTypes.INTEGER);
		assertEquals(4, vType.getByteOccupation());
		assertEquals(4, vType.getMaxByteOccupation());
		
		//
		vType = getCTInteger();
		assertEquals(vType.getT(), CTType.CTypes.INTEGER);
		assertEquals(4, vType.getByteOccupation());
		assertEquals(4, vType.getMaxByteOccupation());
		
		vType = getCTInt();
		assertEquals(vType.getT(), CTType.CTypes.INTEGER);
		assertEquals(4, vType.getByteOccupation());
		assertEquals(4, vType.getMaxByteOccupation());
	}
	
	@Test
	public void TTypes_Builder_Double_Ok()
	{
		TType vType;
		
		vType = getJavaTDouble();
		assertEquals(vType.getT(), JavaTType.JavaTypes.DOUBLE);
		assertEquals(8, vType.getByteOccupation());
		assertEquals(8, vType.getMaxByteOccupation());
		
		vType = getCTDouble();
		assertEquals(vType.getT(), CTType.CTypes.DOUBLE);
		assertEquals(8, vType.getByteOccupation());
		assertEquals(8, vType.getMaxByteOccupation());
	}
	
	@Test
	public void JavaTTypes_Equal_Ok()
	{
		TType vTypeJ, vActualType;
		vTypeJ = getJavaTString();
		
		assertFalse(vTypeJ.equals(null));
		assertTrue(vTypeJ.equals(vTypeJ));
		
		vActualType = getJavaTString();
		
		assertTrue(vTypeJ.equals(vActualType));
		assertTrue( vActualType.equals(vTypeJ));
		
		vActualType = getJavaTInteger();
		
		assertFalse(vTypeJ.equals(vActualType));
		assertFalse( vActualType.equals(vTypeJ));
	}
	
	@Test
	public void CTTypes_Equal_Ok()
	{
		TType vTypeC, vActualType;
		vTypeC = getCTString();
		
		assertFalse(vTypeC.equals(null));
		assertTrue(vTypeC.equals(vTypeC));
		
		vActualType = getCTString();
		
		assertTrue(vTypeC.equals(vActualType));
		assertTrue( vActualType.equals(vTypeC));
		
		vActualType = getCTInteger();
		
		assertFalse(vTypeC.equals(vActualType));
		assertFalse( vActualType.equals(vTypeC));
	}
	
	@Test
	public void TTypes_Builder_BuildsFromOtherTTypes_Ok()
	{
		TType vTypeJ, vTypeC, vActualType;
		
		vTypeJ = getJavaTString();
		vTypeC = getCTString();
		
		//Java from Java
		vActualType = new JavaTType.Builder(vTypeJ).build();
		
		assertEquals(vTypeJ, vActualType);
		assertTrue(vActualType.equals(vTypeJ));
		
		//Java from C
		vActualType = new JavaTType.Builder(vTypeC).build();
		
		assertEquals(vTypeJ, vActualType);
		assertTrue(vActualType.equals(vTypeJ));
		
		//C from Java
		vActualType = new CTType.Builder(vTypeJ).build();
		
		assertEquals(vTypeC, vActualType);
		assertTrue(vActualType.equals(vTypeC));
		
		//C from C
		vActualType = new CTType.Builder(vTypeC).build();
		
		assertEquals(vTypeC, vActualType);
		assertTrue(vActualType.equals(vTypeC));
	}
}
