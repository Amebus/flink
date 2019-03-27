package org.apache.flink.streaming.api.serialization;

import org.apache.flink.api.serialization.StreamReader;
import org.apache.flink.api.serialization.bigendian.BigEndianStreamReader;
import org.apache.flink.api.serialization.reader.IStreamReaderIterator;
import org.apache.flink.api.serialization.reader.StreamIterator;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.junit.Test;

import static org.apache.flink.streaming.helpers.Constants.*;
import static org.apache.flink.streaming.helpers.StreamsGetter.getStreamReaderFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamReaderTest
{
	@Test
	public void Read_Tuple1Ocl00_Error()
	{
		boolean vError = false;
		
		try
		{
			new BigEndianStreamReader().setStream(new byte[] {100 }).getTupleList();
		}
		catch (IllegalArgumentException ex)
		{
			assertEquals(StreamReader.DIMENSION_ERROR, ex.getMessage());
			vError = true;
		}
		
		assertTrue(vError);
	}
	
	@Test
	public void Read_UnsupportedT_Error()
	{
		boolean vError = false;
		
		try
		{
			new BigEndianStreamReader().setStream(new byte[] { 1, 100, 1 }).getTupleList();
		}
		catch (IllegalArgumentException ex)
		{
			assertEquals(StreamIterator.DESERIALIZATION_ERROR, ex.getMessage());
			vError = true;
		}
		
		assertTrue(vError);
	}
	
	@Test
	public void ReadTuple1Ocl_Integer_Ok()
	{
		Tuple1Ocl<Integer> vTuple1Ocl = new Tuple1Ocl<>(ITV_0);
		Tuple1Ocl<Integer> vTuple2Ocl;
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
	}
	
	@Test
	public void ReadTuple1Ocl_Double_Ok()
	{
		Tuple1Ocl<Double> vTuple1Ocl = new Tuple1Ocl<>(DTV_0);
		Tuple1Ocl<Double> vTuple2Ocl;
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
	}
	
	@Test
	public void ReadTuple1Ocl_String_Ok()
	{
		Tuple1Ocl<String> vTuple1Ocl = new Tuple1Ocl<>(STV_0);
		Tuple1Ocl<String> vTuple2Ocl;
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
	}
	
	@Test
	public void ReadTuple2Ocl_IntegerInteger_Ok()
	{
		Tuple2Ocl<Integer, Integer> vTuple1Ocl = new Tuple2Ocl<>(ITV_1, ITV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Integer, Integer> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	@Test
	public void ReadTuple2Ocl_IntegerDouble_Ok()
	{
		Tuple2Ocl<Integer, Double> vTuple1Ocl = new Tuple2Ocl<>(ITV_1, DTV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Integer, Double> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_IntegerString_Ok()
	{
		Tuple2Ocl<Integer, String> vTuple1Ocl = new Tuple2Ocl<>(ITV_1, STV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Integer, String> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_DoubleInteger_Ok()
	{
		Tuple2Ocl<Double, Integer> vTuple1Ocl = new Tuple2Ocl<>(DTV_1, ITV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Double, Integer> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_DoubleDouble_Ok()
	{
		Tuple2Ocl<Double, Double> vTuple1Ocl = new Tuple2Ocl<>(DTV_1, DTV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Double, Double> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_DoubleString_Ok()
	{
		Tuple2Ocl<Double, String> vTuple1Ocl = new Tuple2Ocl<>(DTV_1, STV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<Double, String> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_StringString_Ok()
	{
		Tuple2Ocl<String, String> vTuple1Ocl = new Tuple2Ocl<>(STV_1, STV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<String, String> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	
	@Test
	public void ReadTuple2Ocl_StringInteger_Ok()
	{
		Tuple2Ocl<String, Integer> vTuple1Ocl = new Tuple2Ocl<>(STV_1, ITV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<String, Integer> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
	
	@Test
	public void ReadTuple2Ocl_StringDouble_Ok()
	{
		Tuple2Ocl<String, Double> vTuple1Ocl = new Tuple2Ocl<>(STV_1, DTV_2);
		
		IStreamReaderIterator vIterator = getStreamReaderFrom(vTuple1Ocl).streamReaderIterator();
		assertTrue(vIterator.hasNext());
		Tuple2Ocl<String, Double> vTuple2Ocl = vIterator.nextTuple();
		
		assertEquals(vTuple1Ocl.getArityOcl(), vTuple2Ocl.getArityOcl());
		assertEquals(vTuple1Ocl.getFieldOcl(0), vTuple2Ocl.getFieldOcl(0));
		assertEquals(vTuple1Ocl.getFieldOcl(1), vTuple2Ocl.getFieldOcl(1));
	}
}
